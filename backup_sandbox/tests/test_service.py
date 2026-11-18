"""服务级测试：链路规划、缺段、清理抢先、共用备份、并发、重试不重复占用。

运行：python -m unittest backup_sandbox.tests.test_service -v
"""
import os
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from backup_sandbox.clock import normalize
from backup_sandbox.service import BackupService, SandboxError


def iso(dt: datetime) -> str:
    return normalize(dt.isoformat())


BASE = datetime(2026, 11, 18, 0, 0, 0, tzinfo=timezone.utc)
T = lambda h, m=0: iso(BASE + timedelta(hours=h, minutes=m))  # noqa: E731


class ServiceTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, 'sandbox.db')
        self.svc = BackupService(self.db, step_delay=0)
        self.svc.pause()  # 默认关 worker，测试自己驱动 tick

    def tearDown(self):
        self.svc.close()
        self.tmp.cleanup()

    def full(self, name, h, expire_hours=None, **kw):
        return self.svc.register_backup({
            'name': name, 'kind': 'full', 'taken_at': T(h),
            'expires_at': T(expire_hours) if expire_hours is not None else None,
            'size_kb': 100, **kw})

    def incr(self, name, parent, h, expire_hours=None):
        return self.svc.register_backup({
            'name': name, 'kind': 'incremental', 'taken_at': T(h),
            'expires_at': T(expire_hours) if expire_hours is not None else None,
            'parent_id': parent['id'], 'size_kb': 10})

    def run_to_end(self):
        self.svc.resume()
        for _ in range(1000):
            if not self.svc.worker_tick():
                break
        self.svc.pause()


class TestChainAndGaps(ServiceTestBase):
    def test_full_plus_incremental_chain_recovers_to_target(self):
        f = self.full('f1', 1)
        i1 = self.incr('i1', f, 2)
        i2 = self.incr('i2', i1, 3)
        d = self.svc.create_drill({'label': 'd', 'target_time': T(4)})
        self.run_to_end()
        d = self.svc.get_drill(d['id'])
        self.assertEqual(d['status'], 'completed')
        self.assertEqual(d['recovered_time'], T(3))
        self.assertIsNone(d['gap_kind'])
        self.assertEqual([s['backup_id'] for s in d['steps']],
                         [f['id'], i1['id'], i2['id']])
        self.assertTrue(all(s['status'] == 'done' for s in d['steps']))

    def test_target_between_segments_recovers_to_last_segment_before_target(self):
        f = self.full('f1', 1)
        self.incr('i1', f, 4)
        d = self.svc.create_drill({'target_time': T(3)})
        self.run_to_end()
        d = self.svc.get_drill(d['id'])
        self.assertEqual(d['status'], 'completed')
        self.assertEqual(d['recovered_time'], T(1))
        self.assertEqual(len(d['steps']), 1)

    def test_missing_full_is_first_gap_and_fails(self):
        d = self.svc.create_drill({'target_time': T(2)})
        self.run_to_end()
        d = self.svc.get_drill(d['id'])
        self.assertEqual(d['status'], 'failed')
        self.assertEqual(d['gap_kind'], 'missing_full')
        self.assertIsNone(d['recovered_time'])
        self.assertEqual(d['steps'], [])

    def test_expired_backup_rejected_for_new_drill_as_gap(self):
        future = datetime.now(timezone.utc) + timedelta(days=2)
        ft = lambda h: iso(future + timedelta(hours=h))  # noqa: E731
        f = self.svc.register_backup({
            'name': 'f-old', 'kind': 'full', 'taken_at': ft(0),
            'expires_at': iso(datetime.now(timezone.utc) - timedelta(seconds=1))})
        self.assertTrue(self.svc.get_backup(f['id'])['expired'])
        d = self.svc.create_drill({'target_time': ft(5)})
        self.run_to_end()
        d = self.svc.get_drill(d['id'])
        # 全量本身过期：过期段不入可执行链，一步未还原 => failed，缺段指向该全量
        self.assertEqual(d['status'], 'failed')
        self.assertEqual(d['gap_kind'], 'expired')
        self.assertEqual(d['gap_backup_id'], f['id'])
        self.assertEqual(d['steps'], [])

    def test_expired_mid_chain_incremental_causes_partial_recovery(self):
        # 用未来时间构造“尚未过期”的链，并让中间增量在演练受理前过期
        future = datetime.now(timezone.utc) + timedelta(days=2)
        ft = lambda h: iso(future + timedelta(hours=h))  # noqa: E731
        f = self.svc.register_backup({'name': 'F', 'kind': 'full',
                                      'taken_at': ft(0), 'expires_at': ft(48)})
        i1 = self.svc.register_backup({'name': 'I1', 'kind': 'incremental',
                                       'taken_at': ft(1), 'parent_id': f['id'],
                                       'expires_at': ft(2)})  # 最先过期
        self.svc.register_backup({'name': 'I2', 'kind': 'incremental',
                                  'taken_at': ft(3), 'parent_id': i1['id'],
                                  'expires_at': ft(48)})
        # 手动把 I1 改成已过期
        self.svc.conn.execute('UPDATE backups SET expires_at=? WHERE id=?',
                              (iso(datetime.now(timezone.utc) - timedelta(seconds=1)),
                               i1['id']))
        self.svc.conn.commit()
        d = self.svc.create_drill({'target_time': ft(4)})
        self.run_to_end()
        d = self.svc.get_drill(d['id'])
        self.assertEqual(d['status'], 'partial')
        self.assertEqual(d['gap_kind'], 'expired')
        self.assertEqual(d['gap_backup_id'], i1['id'])
        self.assertEqual(d['recovered_time'], ft(0))  # 只还原了全量
        self.assertEqual([s['status'] for s in d['steps']], ['done'])


class TestCleanupAndSharing(ServiceTestBase):
    def _chain(self):
        f = self.full('f1', 1)
        i1 = self.incr('i1', f, 2)
        i2 = self.incr('i2', i1, 3)
        return f, i1, i2

    def test_cleanup_deletes_expired_unused_backup(self):
        past = iso(datetime.now(timezone.utc) - timedelta(hours=2))
        f = self.svc.register_backup({
            'name': 'f-exp', 'kind': 'full', 'taken_at': past,
            'expires_at': iso(datetime.now(timezone.utc) - timedelta(seconds=1))})
        r = self.svc.cleanup_expired()
        self.assertIn(f['id'], r['deferred_backup_ids'] + r['deleted_backup_ids'])
        names = [b['name'] for b in self.svc.list_backups()]
        self.assertNotIn('f-exp', names)

    def test_cleanup_defers_in_use_backup_until_last_drill_finishes(self):
        future = datetime.now(timezone.utc) + timedelta(days=3)
        ft = lambda h: iso(future + timedelta(hours=h))  # noqa: E731
        f = self.svc.register_backup({'name': 'F', 'kind': 'full',
                                      'taken_at': ft(0), 'expires_at': ft(10)})
        i1 = self.svc.register_backup({'name': 'I1', 'kind': 'incremental',
                                       'taken_at': ft(1), 'parent_id': f['id'],
                                       'expires_at': ft(10)})
        d1 = self.svc.create_drill({'label': 'd1', 'target_time': ft(2)})
        d2 = self.svc.create_drill({'label': 'd2', 'target_time': ft(2)})
        # 演练已受理并占用 F,I1；现在让两个备份过期
        past = iso(datetime.now(timezone.utc) - timedelta(seconds=1))
        self.svc.conn.execute('UPDATE backups SET expires_at=?', (past,))
        self.svc.conn.commit()

        r = self.svc.cleanup_expired()
        # 根 F 的整棵子树（F+I1）均在用：延后列表只登记根，后裔并入同一棵子树
        self.assertEqual(r['deferred_backup_ids'], [f['id']])
        self.assertEqual(r['deleted_backup_ids'], [])
        # 备份仍在，且整棵子树标记 pending_delete
        ids = {b['id']: b for b in self.svc.list_backups()}
        self.assertTrue(ids[f['id']]['pending_delete'])
        self.assertTrue(ids[i1['id']]['pending_delete'])
        self.assertEqual(ids[f['id']]['in_use'], 2)

        self.run_to_end()
        # 最后一场演练结束后自动清扫
        remaining = {b['id'] for b in self.svc.list_backups()}
        self.assertNotIn(f['id'], remaining)
        self.assertNotIn(i1['id'], remaining)
        for did in (d1['id'], d2['id']):
            dd = self.svc.get_drill(did)
            self.assertEqual(dd['status'], 'completed')
        # 历史步骤仍留审计名
        steps = self.svc.get_drill(d1['id'])['steps']
        self.assertEqual([s['backup_name'] for s in steps], ['F', 'I1'])
        self.assertTrue(all(s['backup_id'] is None for s in steps))

    def test_shared_backup_reserved_once_per_drill_and_reused_across_drills(self):
        f, i1, i2 = self._chain()
        d1 = self.svc.create_drill({'label': 'a', 'target_time': T(4)})
        d2 = self.svc.create_drill({'label': 'b', 'target_time': T(4)})
        for did in (d1['id'], d2['id']):
            dd = self.svc.get_drill(did)
            self.assertEqual(dd['reservation_count'], 3)
        # 备份视角：被两场活跃演练共用
        fv = self.svc.get_backup(f['id'])
        self.assertEqual(fv['in_use'], 2)
        # 物理占用行数：每场演练每备份恰一行
        n = self.svc.conn.execute('SELECT COUNT(*) c FROM reservations').fetchone()['c']
        self.assertEqual(n, 6)

    def test_retry_does_not_duplicate_reservations(self):
        f, i1, _ = self._chain()
        d = self.svc.create_drill({'label': 'r', 'target_time': T(4)})
        before = self.svc.get_drill(d['id'])['reservation_count']
        # 模拟中断后续跑
        self.svc.conn.execute("UPDATE drills SET status='interrupted' WHERE id=?", (d['id'],))
        self.svc.conn.commit()
        retried = self.svc.retry_drill(d['id'])
        self.assertEqual(retried['reservation_count'], before)
        self.run_to_end()
        d = self.svc.get_drill(d['id'])
        self.assertEqual(d['status'], 'completed')
        self.assertEqual(d['reservation_count'], before)
        # 每个步骤只执行一次（无重复还原）
        execs = self.svc.conn.execute(
            "SELECT seq, COUNT(*) c FROM drill_steps WHERE status='done' GROUP BY seq"
        ).fetchall()
        self.assertTrue(all(r['c'] == 1 for r in execs))

    def test_retry_only_allowed_from_terminal_or_interrupted(self):
        f = self.full('f1', 1)
        d = self.svc.create_drill({'target_time': T(2)})
        with self.assertRaises(SandboxError):
            self.svc.retry_drill(d['id'])
        self.run_to_end()
        # completed 不能重试
        with self.assertRaises(SandboxError):
            self.svc.retry_drill(d['id'])


    def test_cleanup_race_truncates_new_drill_chain_and_flags_short_of_target(self):
        """清理抢先：无占用的过期增量被删后，新演练链截短，显式提示未到目标时点。"""
        future = datetime.now(timezone.utc) + timedelta(days=4)
        ft = lambda h: iso(future + timedelta(hours=h))  # noqa: E731
        f = self.svc.register_backup({'name': 'F', 'kind': 'full',
                                      'taken_at': ft(0), 'expires_at': ft(48)})
        i1 = self.svc.register_backup({'name': 'I1', 'kind': 'incremental',
                                       'taken_at': ft(1), 'parent_id': f['id'],
                                       'expires_at': ft(48)})
        i2 = self.svc.register_backup({'name': 'I2', 'kind': 'incremental',
                                       'taken_at': ft(2), 'parent_id': i1['id'],
                                       'expires_at': ft(48)})
        # I1/I2 提前过期，且无任何演练占用 => 清理整棵子树
        past = iso(datetime.now(timezone.utc) - timedelta(seconds=1))
        self.svc.conn.execute(
            'UPDATE backups SET expires_at=? WHERE id IN (?,?)',
            (past, i1['id'], i2['id']))
        self.svc.conn.commit()
        r = self.svc.cleanup_expired()
        self.assertEqual(sorted(r['deleted_backup_ids']),
                         sorted([i1['id'], i2['id']]))
        # 新演练只能从全量起链，恢复时点早于目标
        d = self.svc.create_drill({'target_time': ft(3)})
        self.run_to_end()
        d = self.svc.get_drill(d['id'])
        self.assertEqual(d['status'], 'completed')
        self.assertEqual(d['recovered_time'], ft(0))
        self.assertTrue(d['short_of_target'])


    def test_history_keeps_recovered_time_after_backup_deleted(self):
        """演练结束后备份被清理，历史演练的恢复时点与步骤名仍可复核。"""
        past = iso(datetime.now(timezone.utc) - timedelta(hours=3))
        f = self.svc.register_backup({
            'name': 'gone-F', 'kind': 'full', 'taken_at': past,
            'expires_at': iso(datetime.now(timezone.utc) - timedelta(seconds=1))})
        # 备份未过期时先受理并跑完一场演练
        self.svc.conn.execute(
            'UPDATE backups SET expires_at=NULL WHERE id=?', (f['id'],))
        self.svc.conn.commit()
        d = self.svc.create_drill({'target_time': past})
        self.run_to_end()
        d = self.svc.get_drill(d['id'])
        self.assertEqual(d['status'], 'completed')
        recovered = d['recovered_time']
        # 再让它过期并清理
        self.svc.conn.execute(
            'UPDATE backups SET expires_at=? WHERE id=?',
            (iso(datetime.now(timezone.utc) - timedelta(seconds=1)), f['id']))
        self.svc.conn.commit()
        self.svc.cleanup_expired()
        hist = self.svc.get_drill(d['id'])
        self.assertEqual(hist['recovered_time'], recovered)
        self.assertEqual(hist['steps'][0]['backup_name'], 'gone-F')
        self.assertIsNone(hist['steps'][0]['backup_id'])
        self.assertEqual(hist['steps'][0]['backup_time'], recovered)


class TestConcurrency(ServiceTestBase):
    def test_concurrent_cleanup_and_intake(self):
        """清理与演练受理并发：结果等价于串行，无唯一键冲突、无半状态。"""
        f = self.full('f1', 1)
        for h in (2, 3, 4):
            last = self.svc.list_backups()[-1]
            self.incr(f'i{h}', last, h)
        errors = []

        def intake():
            try:
                for k in range(20):
                    self.svc.create_drill(
                        {'label': f'c{k}', 'target_time': T(5)})
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        def cleanup():
            try:
                for _ in range(20):
                    self.svc.cleanup_expired()
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        t1 = threading.Thread(target=intake)
        t2 = threading.Thread(target=cleanup)
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(errors, [])
        rows = self.svc.conn.execute(
            'SELECT drill_id, backup_id, COUNT(*) c FROM reservations '
            'GROUP BY drill_id, backup_id HAVING c>1').fetchall()
        self.assertEqual(rows, [])
        self.assertEqual(len(self.svc.list_drills()), 20)

    def test_concurrent_drill_intake_serialized_by_lock(self):
        f = self.full('f1', 1)
        errors = []

        def fire():
            try:
                for k in range(25):
                    self.svc.create_drill(
                        {'label': f'x-{threading.get_ident()}-{k}',
                         'target_time': T(2)})
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        ts = [threading.Thread(target=fire) for _ in range(4)]
        for t in ts: t.start()
        for t in ts: t.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(self.svc.list_drills()), 100)


class TestValidation(ServiceTestBase):
    def test_incremental_requires_parent_and_ordering(self):
        with self.assertRaises(SandboxError):
            self.svc.register_backup(
                {'name': 'i', 'kind': 'incremental', 'taken_at': T(1)})
        f = self.full('f', 1)
        with self.assertRaises(SandboxError):
            self.incr('bad', f, 0)  # 早于父
        with self.assertRaises(SandboxError):
            self.svc.register_backup(
                {'name': 'f2', 'kind': 'full', 'taken_at': T(2),
                 'parent_id': f['id']})

    def test_duplicate_name_conflict(self):
        self.full('dup', 1)
        with self.assertRaises(SandboxError) as ctx:
            self.full('dup', 2)
        self.assertEqual(ctx.exception.http, 409)


if __name__ == '__main__':
    unittest.main()
