"""演示种子：python -m backup_sandbox.scripts.seed_demo --db PATH

自然键查重，重复执行不覆盖已有数据。构造：
* 一条健康的 全量+2 增量链（未来时点，未过期）
* 一条含已过期增量的链
* 一场覆盖清理抢先/缺段的演练
"""
import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from backup_sandbox.clock import normalize
from backup_sandbox.service import BackupService, SandboxError


def iso(dt):
    return normalize(dt.isoformat())


def seed(db_path: str) -> None:
    svc = BackupService(db_path)  # 默认不启 worker 线程，脚本只做登记
    svc.pause()
    base = datetime.now(timezone.utc) + timedelta(days=1)
    existing = {b['name'] for b in svc.list_backups()}

    def reg(payload):
        if payload['name'] in existing:
            print(f"跳过已存在备份: {payload['name']}")
            return svc.conn.execute(
                'SELECT * FROM backups WHERE name=?', (payload['name'],)).fetchone()
        return svc.register_backup(payload)

    # 健康链
    f1 = reg({'name': 'full-20261117-2200', 'kind': 'full',
              'taken_at': iso(base), 'expires_at': iso(base + timedelta(days=7)),
              'size_kb': 4096})
    i1 = reg({'name': 'incr-20261118-0200', 'kind': 'incremental',
              'parent_id': f1['id'], 'taken_at': iso(base + timedelta(hours=4)),
              'expires_at': iso(base + timedelta(days=7)), 'size_kb': 256})
    i2 = reg({'name': 'incr-20261118-0600', 'kind': 'incremental',
              'parent_id': i1['id'], 'taken_at': iso(base + timedelta(hours=8)),
              'expires_at': iso(base + timedelta(days=7)), 'size_kb': 312})

    # 含过期段的链（补录登记时已过期的历史备份）
    old = reg({'name': 'full-20261110-old', 'kind': 'full',
               'taken_at': iso(datetime.now(timezone.utc) - timedelta(days=8)),
               'expires_at': iso(datetime.now(timezone.utc) - timedelta(days=1)),
               'size_kb': 3000})
    reg({'name': 'incr-20261111-old', 'kind': 'incremental',
         'parent_id': old['id'],
         'taken_at': iso(datetime.now(timezone.utc) - timedelta(days=7)),
         'expires_at': iso(datetime.now(timezone.utc) - timedelta(days=1)),
         'size_kb': 128})

    labels = {d['label'] for d in svc.list_drills()}
    if 'demo-恢复到0600' not in labels:
        d = svc.create_drill({'label': 'demo-恢复到0600',
                              'target_time': iso(base + timedelta(hours=8))})
        print(f"已发起演练 #{d['id']}：健康链恢复到最后一个增量(0600)")
    if 'demo-过期链缺段' not in labels:
        # 目标时点落在“过期全量之后、健康全量(明天)之前”，过期全量是唯一可选全量 => 首个缺段
        d2 = svc.create_drill({'label': 'demo-过期链缺段',
                               'target_time': iso(datetime.now(timezone.utc) + timedelta(minutes=5))})
        print(f"已发起演练 #{d2['id']}：全量已过期，首个缺段为 full-20261110-old")
    print(f'种子完成，DB: {db_path}')
    svc.close()


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default=os.environ.get('SANDBOX_DB', './backup_sandbox.db'))
    seed(ap.parse_args().db)
