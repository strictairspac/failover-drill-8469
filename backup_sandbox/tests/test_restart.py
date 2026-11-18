"""重启恢复测试：模拟进程崩溃后用新实例重开同一 DB。

覆盖：
* 崩溃时 running 的演练在新实例启动时标记 interrupted，原链、已完成段保留；
* 续跑后完成，恢复到的时点正确；
* 重试/重启均不重复占用备份（reservations 行数不变，每段只执行一次）；
* 崩溃瞬间不留半段（pending 段在新实例重跑，done 段不重跑）。
"""
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from backup_sandbox.clock import normalize
from backup_sandbox.service import BackupService

iso = lambda dt: normalize(dt.isoformat())  # noqa: E731


class TestRestartRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, 'sandbox.db')

    def tearDown(self):
        self.tmp.cleanup()

    def _seed_chain(self, svc):
        base = datetime.now(timezone.utc) + timedelta(days=1)
        f = svc.register_backup({'name': 'F', 'kind': 'full',
                                 'taken_at': iso(base), 'expires_at': iso(base + timedelta(days=5))})
        i1 = svc.register_backup({'name': 'I1', 'kind': 'incremental', 'parent_id': f['id'],
                                  'taken_at': iso(base + timedelta(hours=1)),
                                  'expires_at': iso(base + timedelta(days=5))})
        i2 = svc.register_backup({'name': 'I2', 'kind': 'incremental', 'parent_id': i1['id'],
                                  'taken_at': iso(base + timedelta(hours=2)),
                                  'expires_at': iso(base + timedelta(days=5))})
        return base, f, i1, i2

    def test_crash_mid_drill_new_process_resumes_chain_and_progress(self):
        # ---- 实例 A：受理演练，只跑完第一段就“崩溃” ----
        svc_a = BackupService(self.db, step_delay=0)  # 不启 worker 线程，手动 tick
        base, f, i1, i2 = self._seed_chain(svc_a)
        d = svc_a.create_drill({'label': 'night',
                                'target_time': iso(base + timedelta(hours=3))})
        # 手动推进恰好一段
        self.assertTrue(svc_a.worker_tick())
        d_mid = svc_a.get_drill(d['id'])
        self.assertEqual(d_mid['status'], 'running')
        self.assertEqual([s['status'] for s in d_mid['steps']],
                         ['done', 'pending', 'pending'])
        reservations_before = svc_a.conn.execute(
            'SELECT COUNT(*) c FROM reservations').fetchone()['c']
        svc_a.crash()  # 仅记录审计，不做善后；进程直接消失
        svc_a.close()

        # ---- 实例 B：新“进程”打开同一 DB ----
        svc_b = BackupService(self.db, step_delay=0)  # 默认不启线程，手动 tick
        d_b = svc_b.get_drill(d['id'])
        self.assertEqual(d_b['status'], 'interrupted')  # 启动恢复标记
        # 原链与已完成进度原样保留
        self.assertEqual([s['backup_name'] for s in d_b['steps']], ['F', 'I1', 'I2'])
        self.assertEqual([s['status'] for s in d_b['steps']],
                         ['done', 'pending', 'pending'])
        # 占用未重复
        self.assertEqual(svc_b.conn.execute(
            'SELECT COUNT(*) c FROM reservations').fetchone()['c'],
            reservations_before)

        # 显式重试（也可由 worker 直接捞起 interrupted）
        svc_b.retry_drill(d['id'])
        svc_b.resume()
        for _ in range(100):
            if not svc_b.worker_tick():
                break
        done = svc_b.get_drill(d['id'])
        self.assertEqual(done['status'], 'completed')
        self.assertEqual(done['recovered_time'],
                         iso(base + timedelta(hours=2)))
        self.assertTrue(all(s['status'] == 'done' for s in done['steps']))
        # 每段只执行一次
        rows = svc_b.conn.execute(
            'SELECT seq, COUNT(*) c FROM drill_steps GROUP BY seq').fetchall()
        self.assertTrue(all(r['c'] == 1 for r in rows))
        # 占用总数始终未增加
        self.assertEqual(svc_b.conn.execute(
            'SELECT COUNT(*) c FROM reservations').fetchone()['c'],
            reservations_before)
        svc_b.close()

    def test_restart_with_queued_drill_keeps_full_plan(self):
        svc_a = BackupService(self.db, step_delay=0)  # 不启 worker 线程，手动 tick
        base, f, i1, i2 = self._seed_chain(svc_a)
        d = svc_a.create_drill({'target_time': iso(base + timedelta(hours=3))})
        svc_a.close()  # 未执行任何段即退出
        svc_b = BackupService(self.db, step_delay=0)
        svc_b.resume()
        for _ in range(100):
            if not svc_b.worker_tick():
                break
        out = svc_b.get_drill(d['id'])
        self.assertEqual(out['status'], 'completed')
        self.assertEqual(len(out['steps']), 3)
        svc_b.close()

    def test_repeated_restarts_do_not_duplicate_reservations(self):
        svc = BackupService(self.db, step_delay=0)
        svc.pause()
        base, *_ = self._seed_chain(svc)
        d = svc.create_drill({'target_time': iso(base + timedelta(hours=3))})
        expected = svc.get_drill(d['id'])['reservation_count']
        svc.close()
        for _ in range(3):  # 连开三次，每次都跑启动恢复
            s = BackupService(self.db, step_delay=0)
            self.assertEqual(s.get_drill(d['id'])['reservation_count'], expected)
            s.close()


if __name__ == '__main__':
    unittest.main()
