"""HTTP 层：stdlib ThreadingHTTPServer，JSON API + 静态前端。"""
import json
import mimetypes
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from .service import BackupService, SandboxError

STATIC_DIR = Path(__file__).parent / 'web'


def _read_json(handler: 'Handler') -> dict:
    length = int(handler.headers.get('Content-Length') or 0)
    if length == 0:
        return {}
    raw = handler.rfile.read(length)
    try:
        data = json.loads(raw.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise SandboxError('请求体不是合法 JSON', 'bad_json', 400)
    if not isinstance(data, dict):
        raise SandboxError('请求体必须是 JSON 对象', 'bad_json', 400)
    return data


class Handler(BaseHTTPRequestHandler):
    @property
    def service(self) -> 'BackupService':
        return self.server.service

    def log_message(self, fmt, *args):  # 安静一点
        pass

    def _send(self, payload, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _file(self, rel: str) -> None:
        target = (STATIC_DIR / rel).resolve()
        if not str(target).startswith(str(STATIC_DIR.resolve())) or not target.is_file():
            self._send({'error': {'code': 'not_found', 'message': '静态资源不存在'}}, 404)
            return
        ctype = mimetypes.guess_type(str(target))[0] or 'application/octet-stream'
        body = target.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path
        try:
            if path == '/health':
                return self._send({'status': 'ok'})
            if path == '/api/backups':
                return self._send(self.service.list_backups())
            if path == '/api/drills':
                return self._send(self.service.list_drills())
            if path == '/api/events':
                return self._send(self.service.list_events())
            m = __import__('re').fullmatch(r'/api/drills/(\d+)', path)
            if m:
                return self._send(self.service.get_drill(int(m.group(1))))
            if path in ('/', '/index.html'):
                return self._file('index.html')
            if path.startswith('/static/'):
                return self._file(path[len('/static/'):])
            self._send({'error': {'code': 'not_found', 'message': f'无此路由 {path}'}}, 404)
        except SandboxError as e:
            self._send({'error': {'code': e.code, 'message': str(e)}}, e.http)
        except Exception as e:  # noqa: BLE001
            self._send({'error': {'code': 'internal', 'message': str(e)}}, 500)

    def do_POST(self):
        path = urlparse(self.path).path
        try:
            data = _read_json(self)
            if path == '/api/backups':
                return self._send(self.service.register_backup(data), 201)
            if path == '/api/drills':
                return self._send(self.service.create_drill(data), 201)
            if path == '/api/cleanup':
                return self._send(self.service.cleanup_expired())
            m = __import__('re').fullmatch(r'/api/drills/(\d+)/retry', path)
            if m:
                return self._send(self.service.retry_drill(int(m.group(1))))
            if path == '/api/test/pause':
                self.service.pause()
                return self._send({'paused': True})
            if path == '/api/test/resume':
                self.service.resume()
                return self._send({'paused': False})
            if path == '/api/test/crash':
                return self._send(self.service.crash())
            self._send({'error': {'code': 'not_found', 'message': f'无此路由 {path}'}}, 404)
        except SandboxError as e:
            self._send({'error': {'code': e.code, 'message': str(e)}}, e.http)
        except ValueError as e:
            self._send({'error': {'code': 'bad_request', 'message': str(e)}}, 400)
        except Exception as e:  # noqa: BLE001
            self._send({'error': {'code': 'internal', 'message': str(e)}}, 500)


def build_server(db_path: str, host: str, port: int, step_delay: float = 0.05,
                 start_worker: bool = True) -> ThreadingHTTPServer:
    service = BackupService(db_path, step_delay=step_delay)
    if start_worker:
        service.start_worker()

    class Server(ThreadingHTTPServer):
        daemon_threads = True
        allow_reuse_address = True

    srv = Server((host, port), Handler)
    srv.service = service
    return srv
