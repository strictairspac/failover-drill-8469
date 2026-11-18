"""领域服务：备份登记、依赖链规划、演练执行、清理、中断恢复。

并发口径（覆盖“清理和受理可并发”）：
* 进程内所有读改写状态机的操作走同一把 RLock；SQLite 单连接 + WAL。
* 演练执行的每个还原段在锁外只 sleep（模拟耗时），落库仍在锁内，
  因此清理与演练受理不会看到半成品状态。
* 占用以 reservations 唯一键去重：重试与进程重启都不重复占用备份。
"""
import json
import sqlite3
import threading
import time
import uuid

from .clock import now_iso
from .db import ACTIVE_DRILL, TERMINAL_DRILL, backup_dict, connect, init_db

KINDS = ('full', 'incremental')
STATUSES = ('queued', 'running', 'interrupted', 'partial', 'completed', 'failed')
ACTIVE_STATUSES = ('queued', 'running', 'interrupted')


class SandboxError(Exception):
    def __init__(self, message: str, code: str = 'bad_request', http: int = 400):
        super().__init__(message)
        self.code = code
        self.http = http


class BackupService:
    def __init__(self, db_path: str, step_delay: float = 0.05,
                 worker_interval: float = 0.1):
        self.conn = connect(db_path)
        init_db(self.conn)
        self.lock = threading.RLock()
        self.step_delay = step_delay
        self.worker_interval = worker_interval
        self.paused = False
        self._worker = None
        self._stop = threading.Event()
        self._active_worker_drills: set[int] = set()
        self._recover_on_startup()

    # ---------- 基础工具 ----------
    def _log(self, actor: str, action: str, detail: dict) -> None:
        self.conn.execute(
            'INSERT INTO events(created_at, actor, action, detail) VALUES (?,?,?,?)',
            (now_iso(), actor, action, json.dumps(detail, ensure_ascii=False)))

    # ---------- 启动恢复：把崩溃时 running 的演练标记 interrupted ----------
    def _recover_on_startup(self) -> None:
        with self.lock:
            rows = self.conn.execute(
                "SELECT id FROM drills WHERE status='running'").fetchall()
            for r in rows:
                self.conn.execute(
                    "UPDATE drills SET status='interrupted' WHERE id=? AND status='running'",
                    (r['id'],))
                self._log('system', 'drill-resume',
                          {'drill_id': r['id'], 'reason': '进程重启，原链与进度保留'})
            self.conn.commit()
            # 重启后自动续跑（worker 线程统一推进，含 queued 与 interrupted）

    # ---------- 备份登记 ----------
    def register_backup(self, data: dict, actor: str = 'user') -> dict:
        name = (data.get('name') or '').strip()
        kind = data.get('kind')
        taken_at = data.get('taken_at')
        if not name:
            raise SandboxError('备份名称不能为空')
        if kind not in KINDS:
            raise SandboxError(f'kind 必须是 {KINDS} 之一')
        from .clock import normalize
        taken_at = normalize(taken_at)
        expires_at = normalize(data['expires_at']) if data.get('expires_at') else None
        # 注意：不强制 expires_at > taken_at——备份登记后随时钟自然到期，
        # 也允许补录“登记时已过期”的历史备份；过期与否由当前时钟判定。
        parent_id = data.get('parent_id')
        size_kb = int(data.get('size_kb') or 0)
        if size_kb < 0:
            raise SandboxError('size_kb 不能为负')
        with self.lock:
            if self.conn.execute('SELECT 1 FROM backups WHERE name=?', (name,)).fetchone():
                raise SandboxError(f'备份名 {name} 已存在', 'conflict', 409)
            if kind == 'incremental':
                if not parent_id:
                    raise SandboxError('增量备份必须登记依赖 parent_id')
                parent = self.conn.execute(
                    'SELECT * FROM backups WHERE id=?', (parent_id,)).fetchone()
                if not parent:
                    raise SandboxError('依赖的父备份不存在')
                if parent['taken_at'] >= taken_at:
                    raise SandboxError('增量备份时点必须晚于其父备份')
            elif parent_id:
                raise SandboxError('全量备份不能登记父依赖')
            cur = self.conn.execute(
                """INSERT INTO backups(name, kind, taken_at, expires_at, parent_id,
                   size_kb, pending_delete, created_at) VALUES (?,?,?,?,?,?,0,?)""",
                (name, kind, taken_at, expires_at, parent_id, size_kb, now_iso()))
            self._log(actor, 'backup-register',
                      {'backup_id': cur.lastrowid, 'name': name, 'kind': kind,
                       'parent_id': parent_id})
            self.conn.commit()
            return self.get_backup(cur.lastrowid)

    def get_backup(self, backup_id: int) -> dict:
        with self.lock:
            r = self.conn.execute('SELECT * FROM backups WHERE id=?',
                                  (backup_id,)).fetchone()
            if not r:
                raise SandboxError('备份不存在', 'not_found', 404)
            return self._hydrate(r)

    def _hydrate(self, r) -> dict:
        now = now_iso()
        cnt = self.conn.execute(
            f"""SELECT COUNT(*) c FROM reservations rv JOIN drills d ON d.id=rv.drill_id
                WHERE rv.backup_id=? AND {ACTIVE_DRILL}""", (r['id'],)).fetchone()['c']
        return backup_dict(r, now, in_use=cnt)

    def list_backups(self) -> list[dict]:
        with self.lock:
            rows = self.conn.execute('SELECT * FROM backups ORDER BY taken_at, id').fetchall()
            return [self._hydrate(r) for r in rows]

    # ---------- 恢复链规划 ----------
    def _plan_locked(self, target_time: str) -> tuple[list[sqlite3.Row], dict | None]:
        """返回到 target_time 的恢复链（全量在前、增量按序），及首个缺段信息。"""
        fulls = self.conn.execute(
            """SELECT * FROM backups WHERE kind='full' AND taken_at<=?
               ORDER BY taken_at DESC, id DESC""", (target_time,)).fetchall()
        if not fulls:
            return [], {'gap_kind': 'missing_full', 'gap_backup_id': None,
                        'gap_detail': f'{target_time} 之前没有任何全量备份，无法起链'}
        chain: list[sqlite3.Row] = []
        # 离目标时点最近的全量（同点取最新登记）
        cur = fulls[0]
        # 从该全量沿增量链向新走，收集 taken_at<=target 的增量；
        # 遇过期段立即停止，过期段本身不入可执行链（不能还原过期备份）
        while True:
            if cur['expires_at'] and cur['expires_at'] <= now_iso():
                return chain, {'gap_kind': 'expired', 'gap_backup_id': cur['id'],
                               'gap_detail': f"备份 {cur['name']} 已过期，不能用于新演练，链在此断开"}
            chain.append(cur)
            nxt = self.conn.execute(
                """SELECT * FROM backups WHERE parent_id=? AND taken_at<=?
                   ORDER BY taken_at ASC, id ASC LIMIT 1""",
                (cur['id'], target_time)).fetchone()
            if not nxt:
                break
            if nxt['expires_at'] and nxt['expires_at'] <= now_iso():
                return chain, {'gap_kind': 'expired', 'gap_backup_id': nxt['id'],
                               'gap_detail': f"备份 {nxt['name']} 已过期，增量链在此断开"}
            cur = nxt
        return chain, None

    # ---------- 演练受理 ----------
    def create_drill(self, data: dict, actor: str = 'user') -> dict:
        label = (data.get('label') or f'drill-{uuid.uuid4().hex[:8]}').strip()
        if not data.get('target_time'):
            raise SandboxError('target_time 不能为空')
        from .clock import normalize
        target_time = normalize(data['target_time'])
        with self.lock:
            chain, gap = self._plan_locked(target_time)
            now = now_iso()
            cur = self.conn.execute(
                """INSERT INTO drills(label, target_time, status, plan_frozen_at,
                   created_at, gap_backup_id, gap_kind, gap_detail)
                   VALUES (?,?, 'queued', ?, ?, ?, ?, ?)""",
                (label, target_time, now, now,
                 gap['gap_backup_id'] if gap else None,
                 gap['gap_kind'] if gap else None,
                 gap['gap_detail'] if gap else None))
            drill_id = cur.lastrowid
            for seq, b in enumerate(chain):
                self.conn.execute(
                    """INSERT INTO drill_steps(drill_id, seq, backup_id, backup_name,
                       backup_time, status) VALUES (?,?,?,?,?, 'pending')""",
                    (drill_id, seq, b['id'], b['name'], b['taken_at']))
                # 幂等占用：UNIQUE(drill_id, backup_id) 保证重试/重启不重复
                self.conn.execute(
                    """INSERT OR IGNORE INTO reservations(drill_id, backup_id, created_at)
                       VALUES (?,?,?)""", (drill_id, b['id'], now))
            self._log(actor, 'drill-create',
                      {'drill_id': drill_id, 'label': label, 'target_time': target_time,
                       'segments': len(chain),
                       'gap': gap['gap_kind'] if gap else None})
            self.conn.commit()
            return self.get_drill(drill_id)

    def get_drill(self, drill_id: int) -> dict:
        with self.lock:
            d = self.conn.execute('SELECT * FROM drills WHERE id=?',
                                  (drill_id,)).fetchone()
            if not d:
                raise SandboxError('演练不存在', 'not_found', 404)
            return self._hydrate_drill(d)

    def _hydrate_drill(self, d: sqlite3.Row) -> dict:
        out = dict(d)
        steps = self.conn.execute(
            """SELECT s.id, s.drill_id, s.seq, s.backup_id, s.status, s.executed_at,
                      COALESCE(b.name, s.backup_name) backup_name,
                      b.kind backup_kind,
                      COALESCE(b.taken_at, s.backup_time) backup_time
               FROM drill_steps s LEFT JOIN backups b ON b.id=s.backup_id
               WHERE s.drill_id=? ORDER BY s.seq""", (d['id'],)).fetchall()
        out['steps'] = [dict(s) for s in steps]
        gap_b = self.conn.execute('SELECT id, name FROM backups WHERE id=?',
                                  (d['gap_backup_id'],)).fetchone() if d['gap_backup_id'] else None
        out['gap_backup_name'] = gap_b['name'] if gap_b else None
        out['reservation_count'] = self.conn.execute(
            'SELECT COUNT(*) c FROM reservations WHERE drill_id=?', (d['id'],)).fetchone()['c']
        # 清理抢先删段时链会截短：终态虽完成，但实际恢复时点早于目标时点需显式提示
        out['short_of_target'] = bool(
            out['recovered_time'] and out['recovered_time'] < d['target_time'])
        return out

    def list_drills(self) -> list[dict]:
        with self.lock:
            rows = self.conn.execute('SELECT * FROM drills ORDER BY id DESC').fetchall()
            return [self._hydrate_drill(d) for d in rows]

    # ---------- worker：推进一场演练（每段独立落库，崩溃只丢当前段） ----------
    def worker_tick(self, actor: str = 'worker') -> bool:
        """尝试推进一场演练；返回是否干了活。由后台线程或测试驱动。"""
        if self.paused:
            return False
        with self.lock:
            # 含 running：同一进程内每 tick 推一段，直到没有 pending 段再收尾
            d = self.conn.execute(
                """SELECT * FROM drills WHERE status IN ('queued','running','interrupted')
                   ORDER BY id ASC LIMIT 1""").fetchone()
            if not d:
                return False
            drill_id = d['id']
            if drill_id in self._active_worker_drills:
                return False  # 同一进程内不重复开工
            self._active_worker_drills.add(drill_id)
            self.conn.execute(
                "UPDATE drills SET status='running', started_at=COALESCE(started_at,?) WHERE id=?",
                (now_iso(), drill_id))
            step = self.conn.execute(
                """SELECT * FROM drill_steps WHERE drill_id=? AND status='pending'
                   ORDER BY seq LIMIT 1""", (drill_id,)).fetchone()
            self.conn.commit()
        try:
            if step is None:
                delay = 0.0
                backup_id = None
            else:
                delay = self.step_delay
                backup_id = step['backup_id']
            if delay:
                time.sleep(delay)  # 模拟真实还原耗时；此时清理可并发跑
            with self.lock:
                if step is not None:
                    # 崩溃恢复后仍在 pending 的同一段由本次执行完成；done 段不会被二次执行
                    exists = self.conn.execute(
                        'SELECT status FROM drill_steps WHERE id=?', (step['id'],)).fetchone()
                    if exists and exists['status'] == 'pending':
                        self.conn.execute(
                            "UPDATE drill_steps SET status='done', executed_at=? WHERE id=?",
                            (now_iso(), step['id']))
                        self._log(actor, 'drill-step',
                                  {'drill_id': drill_id, 'backup_id': backup_id,
                                   'seq': step['seq']})
                self._finalize_locked(drill_id, actor)
                self.conn.commit()
        finally:
            with self.lock:
                self._active_worker_drills.discard(drill_id)
        return True

    def _finalize_locked(self, drill_id: int, actor: str) -> None:
        d = self.conn.execute('SELECT * FROM drills WHERE id=?', (drill_id,)).fetchone()
        pending = self.conn.execute(
            "SELECT COUNT(*) c FROM drill_steps WHERE drill_id=? AND status='pending'",
            (drill_id,)).fetchone()['c']
        if pending > 0:
            return  # 还有段没还原，保持 running，下个 tick 继续
        gap = d['gap_kind']
        done = self.conn.execute(
            "SELECT COUNT(*) c FROM drill_steps WHERE drill_id=? AND status='done'",
            (drill_id,)).fetchone()['c']
        if not gap:
            status = 'completed'
        elif done == 0:
            status = 'failed'       # 遇缺段且一步未还原（如全量缺失/全量即过期）
        else:
            status = 'partial'      # 已还原前序段后遇缺段
        last = self.conn.execute(
            """SELECT COALESCE(b.taken_at, s.backup_time) t
               FROM drill_steps s LEFT JOIN backups b ON b.id=s.backup_id
               WHERE s.drill_id=? AND s.status='done' ORDER BY s.seq DESC LIMIT 1""",
            (drill_id,)).fetchone()
        self.conn.execute(
            """UPDATE drills SET status=?, recovered_time=?, finished_at=? WHERE id=?""",
            (status, last['t'] if last else None, now_iso(), drill_id))
        self._log(actor, 'drill-finish',
                  {'drill_id': drill_id, 'status': status,
                   'recovered_time': last['t'] if last else None, 'gap': gap})
        # 演练结束即释放语义：占用仅关联活跃演练；顺带清扫待删备份
        self._sweep_locked(actor)

    # ---------- 重试（不重复占用：占用在受理时已幂等写全） ----------
    def retry_drill(self, drill_id: int, actor: str = 'user') -> dict:
        with self.lock:
            d = self.conn.execute('SELECT * FROM drills WHERE id=?',
                                  (drill_id,)).fetchone()
            if not d:
                raise SandboxError('演练不存在', 'not_found', 404)
            if d['status'] not in ('interrupted', 'failed', 'partial'):
                raise SandboxError(
                    f"演练状态为 {d['status']}，仅 interrupted/failed/partial 可重试",
                    'conflict', 409)
            before = self.conn.execute(
                'SELECT COUNT(*) c FROM reservations WHERE drill_id=?',
                (drill_id,)).fetchone()['c']
            # 原链与进度原样保留，只把状态拨回 queued；reservations 不重写、不新增
            self.conn.execute(
                "UPDATE drills SET status='queued', finished_at=NULL WHERE id=?", (drill_id,))
            after = self.conn.execute(
                'SELECT COUNT(*) c FROM reservations WHERE drill_id=?',
                (drill_id,)).fetchone()['c']
            assert before == after
            self._log(actor, 'drill-retry',
                      {'drill_id': drill_id, 'reservations': after})
            self.conn.commit()
            return self.get_drill(drill_id)

    # ---------- 清理：过期备份不接新演练（规划时拦截）；在用的留到最后一场 ----------
    def _in_use_count_locked(self, backup_id: int) -> int:
        return self.conn.execute(
            f"""SELECT COUNT(*) c FROM reservations rv JOIN drills d ON d.id=rv.drill_id
                WHERE rv.backup_id=? AND {ACTIVE_DRILL}""", (backup_id,)).fetchone()['c']

    def cleanup_expired(self, actor: str = 'user') -> dict:
        with self.lock:
            now = now_iso()
            expired = self.conn.execute(
                'SELECT * FROM backups WHERE expires_at IS NOT NULL AND expires_at<=?',
                (now,)).fetchall()
            marked, deleted, deferred, handled = [], [], [], set()
            for b in expired:
                if b['id'] in handled:
                    continue  # 可能已作为别的过期备份的后裔被整棵处理
                subtree = self._subtree_ids_locked(b['id'])
                handled.update(subtree)
                used = sum(self._in_use_count_locked(x) for x in subtree)
                if used > 0:
                    # 正在使用：整棵子树标记待删，等最后一场占用演练结束
                    for x in subtree:
                        row = self.conn.execute(
                            'SELECT pending_delete p FROM backups WHERE id=?',
                            (x,)).fetchone()
                        if row and not row['p']:
                            self.conn.execute(
                                'UPDATE backups SET pending_delete=1 WHERE id=?', (x,))
                    deferred.append(b['id'])
                else:
                    names = [r['name'] for r in self.conn.execute(
                        f'SELECT name FROM backups WHERE id IN ({",".join("?" for _ in subtree)})',
                        subtree).fetchall()]
                    deleted.extend(self._delete_backup_locked(b['id']))
                    marked.extend(names)
            self._log(actor, 'cleanup',
                      {'deleted': deleted, 'deferred_in_use': deferred})
            self.conn.commit()
            return {'deleted_backup_ids': deleted, 'deleted_names': marked,
                    'deferred_backup_ids': deferred}

    def _subtree_ids_locked(self, backup_id: int) -> list[int]:
        """返回备份及其全部后裔（沿 parent_id 向下），深度优先。"""
        out = []
        stack = [backup_id]
        while stack:
            cur = stack.pop()
            out.append(cur)
            kids = self.conn.execute(
                'SELECT id FROM backups WHERE parent_id=? ORDER BY id', (cur,)).fetchall()
            stack.extend(r['id'] for r in kids)
        return out

    def _delete_backup_locked(self, backup_id: int) -> list[int]:
        # 父备份没了，后裔增量同样不可用 => 整棵子树一起清理
        ids = self._subtree_ids_locked(backup_id)
        # 历史演练步骤保留（backup_name 冗余留存），仅解除外键引用
        qmarks = ','.join('?' for _ in ids)
        self.conn.execute(
            f'UPDATE drill_steps SET backup_id=NULL WHERE backup_id IN ({qmarks})', ids)
        self.conn.execute(
            f'DELETE FROM reservations WHERE backup_id IN ({qmarks})', ids)
        self.conn.execute(f'DELETE FROM backups WHERE id IN ({qmarks})', ids)
        return ids

    def _sweep_locked(self, actor: str) -> None:
        """演练结束后调用：pending_delete 且整棵子树已无活跃占用的备份立即删除。"""
        rows = self.conn.execute(
            'SELECT id FROM backups WHERE pending_delete=1 ORDER BY id').fetchall()
        removed, seen = [], set()
        for r in rows:
            if r['id'] in seen:
                continue
            subtree = self._subtree_ids_locked(r['id'])
            seen.update(subtree)
            if all(self._in_use_count_locked(x) == 0 for x in subtree):
                removed.extend(self._delete_backup_locked(r['id']))
        if removed:
            self._log(actor, 'cleanup-sweep', {'deleted': removed})

    # ---------- 测试/演示控制 ----------
    def pause(self) -> None:
        with self.lock:
            self.paused = True

    def resume(self) -> None:
        with self.lock:
            self.paused = False

    def crash(self, actor: str = 'user') -> dict:
        """模拟崩溃：把 running 演练留成 running（不做善后），由下次启动恢复。"""
        with self.lock:
            running = self.conn.execute(
                "SELECT id FROM drills WHERE status='running'").fetchall()
            self._log(actor, 'test-crash',
                      {'running_drills': [r['id'] for r in running]})
            self.conn.commit()
            return {'running_drills': [r['id'] for r in running]}

    def list_events(self, limit: int = 100) -> list[dict]:
        with self.lock:
            rows = self.conn.execute(
                'SELECT * FROM events ORDER BY id DESC LIMIT ?', (limit,)).fetchall()
            out = []
            for r in rows:
                d = dict(r)
                try:
                    d['detail'] = json.loads(d['detail'])
                except (TypeError, ValueError):
                    pass
                out.append(d)
            return out

    def wait_idle(self, timeout: float = 10.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self.lock:
                busy = self.conn.execute(
                    f"SELECT COUNT(*) c FROM drills WHERE {ACTIVE_DRILL}").fetchone()['c']
            if busy == 0:
                return
            time.sleep(0.02)
        raise TimeoutError('等待演练排空超时')

    # ---------- 后台 worker 线程 ----------
    def start_worker(self) -> None:
        if self._worker and self._worker.is_alive():
            return

        def loop():
            while not self._stop.is_set():
                try:
                    did = self.worker_tick()
                except Exception:
                    did = False
                if not did:
                    self._stop.wait(self.worker_interval)

        self._worker = threading.Thread(target=loop, name='drill-worker', daemon=True)
        self._worker.start()

    def close(self) -> None:
        self._stop.set()
        if self._worker:
            self._worker.join(timeout=2)
        self.conn.close()
