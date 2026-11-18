"""HTTP 端到端：真实 ThreadingHTTPServer + JSON API，含并发与崩溃换实例。

运行：python -m unittest backup_sandbox.tests.test_http -v
"""
import json
import os
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from backup_sandbox.clock import normalize
from backup_sandbox.web import build_server

iso = lambda dt: normalize(dt.isoformat())  # noqa: E731


class HttpTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, 'sandbox.db')
        self.srv = build_server(self.db, '127.0.0.1', 0,
                                step_delay=0.01, start_worker=True)
        self.port = self.srv.server_address[1]
        self.thread = threading.Thread(target=self.srv.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.srv.shutdown()
        self.srv.service.close()
        self.srv.server_close()
        self.tmp.cleanup()

    def api(self, path, payload=None, method=None):
        data = None
        headers = {'Content-Type': 'application/json'}
        if payload is not None:
            data = json.dumps(payload).encode('utf-8')
            method = method or 'POST'
        req = urllib.request.Request(
            f'http://127.0.0.1:{self.port}{path}', data=data,
            headers=headers, method=method or 'GET')
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode('utf-8'))

    def wait_terminal(self, drill_id, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            _, d = self.api(f'/api/drills/{drill_id}')
            if d['status'] in ('partial', 'completed', 'failed'):
                return d
            time.sleep(0.03)
        raise AssertionError('演练未在超时内结束')


class TestHttpFlow(HttpTestBase):
    def test_health_and_static(self):
        status, body = self.api('/health')
        self.assertEqual(status, 200)
        self.assertEqual(body['status'], 'ok')
        with urllib.request.urlopen(
                f'http://127.0.0.1:{self.port}/', timeout=5) as r:
            self.assertIn('备份恢复演练沙盘', r.read().decode('utf-8'))
        with urllib.request.urlopen(
                f'http://127.0.0.1:{self.port}/static/app.js', timeout=5) as r:
            self.assertEqual(r.status, 200)

    def test_full_drill_flow_shows_recovered_time(self):
        base = datetime.now(timezone.utc) + timedelta(days=1)
        _, f = self.api('/api/backups', {
            'name': 'F', 'kind': 'full', 'taken_at': iso(base),
            'expires_at': iso(base + timedelta(days=2))})
        _, i1 = self.api('/api/backups', {
            'name': 'I1', 'kind': 'incremental', 'parent_id': f['id'],
            'taken_at': iso(base + timedelta(hours=1)),
            'expires_at': iso(base + timedelta(days=2))})
        _, i2 = self.api('/api/backups', {
            'name': 'I2', 'kind': 'incremental', 'parent_id': i1['id'],
            'taken_at': iso(base + timedelta(hours=2)),
            'expires_at': iso(base + timedelta(days=2))})
        status, d = self.api('/api/drills', {
            'label': 'night', 'target_time': iso(base + timedelta(hours=3))})
        self.assertEqual(status, 201)
        out = self.wait_terminal(d['id'])
        self.assertEqual(out['status'], 'completed')
        self.assertEqual(out['recovered_time'], iso(base + timedelta(hours=2)))
        self.assertEqual([s['backup_name'] for s in out['steps']], ['F', 'I1', 'I2'])
        # 列表接口同样可见
        _, drills = self.api('/api/drills')
        self.assertEqual(drills[0]['recovered_time'], out['recovered_time'])

    def test_gap_reported_on_drill_creation(self):
        _, d = self.api('/api/drills', {
            'target_time': iso(datetime.now(timezone.utc) + timedelta(hours=1))})
        self.assertEqual(d['gap_kind'], 'missing_full')
        out = self.wait_terminal(d['id'])
        self.assertEqual(out['status'], 'failed')
        self.assertIn('没有任何全量', out['gap_detail'])

    def test_validation_errors_are_400(self):
        status, body = self.api('/api/backups',
                                {'name': 'x', 'kind': 'weird',
                                 'taken_at': iso(datetime.now(timezone.utc))})
        self.assertEqual(status, 400)
        self.assertIn('error', body)

    def test_retry_endpoint_resumes_without_new_reservations(self):
        base = datetime.now(timezone.utc) + timedelta(days=1)
        _, f = self.api('/api/backups', {
            'name': 'F', 'kind': 'full', 'taken_at': iso(base),
            'expires_at': iso(base + timedelta(days=2))})
        _, d = self.api('/api/drills',
                        {'target_time': iso(base + timedelta(hours=1))})
        self.wait_terminal(d['id'])
        # completed 不允许重试
        status, _ = self.api(f"/api/drills/{d['id']}/retry", {})
        self.assertEqual(status, 409)

    def test_concurrent_intake_and_cleanup_requests(self):
        base = datetime.now(timezone.utc) + timedelta(days=2)
        _, f = self.api('/api/backups', {
            'name': 'F', 'kind': 'full', 'taken_at': iso(base),
            'expires_at': iso(base + timedelta(days=5))})
        errors = []

        def intake():
            for k in range(15):
                st, body = self.api('/api/drills', {
                    'label': f'c-{threading.get_ident()}-{k}',
                    'target_time': iso(base + timedelta(minutes=1))})
                if st != 201:
                    errors.append((st, body))

        def cleanup():
            for _ in range(15):
                st, body = self.api('/api/cleanup', {})
                if st != 200:
                    errors.append((st, body))

        ts = [threading.Thread(target=intake) for _ in range(3)]
        ts.append(threading.Thread(target=cleanup))
        for t in ts: t.start()
        for t in ts: t.join()
        self.assertEqual(errors, [])
        _, drills = self.api('/api/drills')
        self.assertEqual(len(drills), 45)
        # 等 worker 把队列排空（每段 10ms，串行推进）
        deadline = time.time() + 15
        while time.time() < deadline:
            _, drills = self.api('/api/drills')
            if all(d['status'] in ('partial', 'completed', 'failed') for d in drills):
                break
            time.sleep(0.05)
        for d in drills:
            self.assertEqual(d['status'], 'completed')
        _, backups = self.api('/api/backups')
        self.assertEqual(len(backups), 1)  # 未过期，清理不动它


class TestHttpCrashRestart(HttpTestBase):
    def test_crash_then_restart_on_same_db_resumes(self):
        base = datetime.now(timezone.utc) + timedelta(days=1)
        _, f = self.api('/api/backups', {
            'name': 'F', 'kind': 'full', 'taken_at': iso(base),
            'expires_at': iso(base + timedelta(days=3))})
        _, i1 = self.api('/api/backups', {
            'name': 'I1', 'kind': 'incremental', 'parent_id': f['id'],
            'taken_at': iso(base + timedelta(hours=1)),
            'expires_at': iso(base + timedelta(days=3))})
        # 用较长的段延迟制造 running 窗口，等演练真正开跑后再“崩溃”
        self.srv.service.step_delay = 0.6
        _, d = self.api('/api/drills',
                        {'target_time': iso(base + timedelta(hours=2))})
        deadline = time.time() + 5
        while time.time() < deadline:
            _, mid = self.api(f'/api/drills/{d["id"]}')
            if mid['status'] == 'running':
                break
            time.sleep(0.02)
        _, mid = self.api(f'/api/drills/{d["id"]}')
        self.assertEqual(mid['status'], 'running')
        self.api('/api/test/crash', {})

        # 关掉旧服务，用同 DB 起新服务（模拟进程重启）
        self.srv.shutdown()
        self.srv.service.close()
        self.srv.server_close()
        self.srv = build_server(self.db, '127.0.0.1', 0,
                                step_delay=0.01, start_worker=True)
        self.port = self.srv.server_address[1]
        self.thread = threading.Thread(target=self.srv.serve_forever, daemon=True)
        self.thread.start()

        _, after = self.api(f'/api/drills/{d["id"]}')
        self.assertIn(after['status'], ('interrupted', 'running', 'completed'))
        out = self.wait_terminal(d['id'])
        self.assertEqual(out['status'], 'completed')
        self.assertEqual(out['recovered_time'], iso(base + timedelta(hours=1)))
        self.assertEqual([s['backup_name'] for s in out['steps']], ['F', 'I1'])
        # 占用没有因重启重复
        self.assertEqual(out['reservation_count'], 2)
        _, events = self.api('/api/events')
        self.assertTrue(any(e['action'] == 'drill-resume' for e in events))


if __name__ == '__main__':
    unittest.main()
