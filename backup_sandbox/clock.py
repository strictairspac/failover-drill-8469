"""时间口径：内部一律存 UTC ISO8601 字符串，字典序即可比较。"""
from datetime import datetime, timezone


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize(ts: str) -> str:
    """接受带偏移或 naive 的 ISO 串，统一转成 UTC ISO；naive 视为 UTC。"""
    if ts is None:
        raise ValueError('时间不能为空')
    s = str(ts).strip()
    if s.endswith('Z'):
        s = s[:-1] + '+00:00'
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def to_local(iso_ts: str | None) -> str:
    if not iso_ts:
        return '—'
    dt = datetime.fromisoformat(iso_ts.replace('Z', '+00:00'))
    return dt.astimezone().strftime('%Y-%m-%d %H:%M:%S')
