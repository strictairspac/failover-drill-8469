"""python -m backup_sandbox [--db PATH] [--host H] [--port P] [--step-delay S]"""
import argparse
import os

from .web import build_server


def main() -> None:
    ap = argparse.ArgumentParser(description='备份恢复演练沙盘')
    ap.add_argument('--db', default=os.environ.get('SANDBOX_DB', './backup_sandbox.db'))
    ap.add_argument('--host', default=os.environ.get('SANDBOX_HOST', '127.0.0.1'))
    ap.add_argument('--port', type=int, default=int(os.environ.get('SANDBOX_PORT', '8249')))
    ap.add_argument('--step-delay', type=float,
                    default=float(os.environ.get('STEP_DELAY', '0.05')))
    args = ap.parse_args()
    srv = build_server(args.db, args.host, args.port, step_delay=args.step_delay)
    print(f'备份恢复演练沙盘: http://{args.host}:{args.port}  (db={args.db})')
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.service.close()
        srv.server_close()


if __name__ == '__main__':
    main()
