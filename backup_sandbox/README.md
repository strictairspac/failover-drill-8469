# 备份恢复演练沙盘

备份一过期，恢复演练跑到一半就可能缺段。本沙盘在受控环境里登记**全量/增量备份及其依赖**，
按指定时点演练恢复，并把清理、占用、中断恢复的规则全部落到可复核的状态机里。

> 纯 Python 3.11 标准库实现（`http.server` + `sqlite3`），**零第三方依赖、零构建步骤**，
> 在没有 Docker/MySQL/Redis 的环境也能直接跑。所有数据落 SQLite（WAL），进程崩溃后从磁盘恢复。

## 启动

```bash
# 演示数据（自然键查重，可重复执行）
python3 -m backup_sandbox.scripts.seed_demo --db ./sandbox.db
# 起服务（默认 127.0.0.1:8249）
python3 -m backup_sandbox
#   可选：--db / --host / --port / --step-delay（每段还原耗时，模拟演练时长）
```

打开 http://127.0.0.1:8249 ，页面可登记备份、发起演练、手动清理，
并显示每场演练**恢复到的时点**或**首个缺段**，每 2 秒自动刷新。

测试：`bash backup_sandbox/scripts/run_tests.sh`（26 项，约 7 秒）。

## 领域模型

| 表 | 作用 |
|---|---|
| `backups` | 备份：`full`/`incremental`、时点、过期时点、`parent_id` 依赖、`pending_delete` |
| `drills` | 演练：目标时点、状态机、恢复时点、首个缺段（kind/备份/原因） |
| `drill_steps` | 冻结的恢复链（受理时规划），每段 pending/done，留存备份名做审计 |
| `reservations` | 演练对备份的占用，`UNIQUE(drill_id, backup_id)` 去重 |
| `events` | 审计：登记/受理/逐段/收尾/清理/中断恢复 |

增量备份必须登记父依赖且时点晚于父备份；恢复链 = 目标时点前最新全量 + 沿 `parent_id`
向新的连续增量。规划结果（链与缺段）在**受理时冻结**，执行期间备份被清理不改变本场演练。

## 规则口径（对应需求）

1. **过期备份不接新演练**：规划时逐段判过期，过期段不进入可执行链，记为首个缺段；
   缺段前的链照常还原。
2. **清理和受理可并发**：所有读改写状态机走同一把进程内 `RLock`，SQLite 单连接 + WAL；
   还原段的耗时 sleep 在锁外，清理不会读到半成品。
3. **正在使用的备份留到最后一场演练结束**：清理时若备份（或其子孙子树）仍被活跃演练
   （queued/running/interrupted）占用，只置 `pending_delete`；每场演练收尾时 `_sweep`，
   等最后一场占用释放后整棵子树删除。历史演练步骤置空外键但保留备份名审计轨迹。
4. **中断后恢复原链和进度**：每段独立事务落库；进程重启时把残留 `running` 标为
   `interrupted`（事件 `drill-resume`），worker 自动捞起 queued/interrupted/running，
   已 done 的段不重跑，只续 pending 段。
5. **重试不重复占用备份**：占用在受理时以 `INSERT OR IGNORE` 幂等写全；重试只把状态拨回
   `queued`，不重写占用；每段在步骤表只有一行、只执行一次。
6. **首个缺段 / 恢复时点**：`missing_full`（无全量起链，failed）、`expired`（过期断链，
   按已还原段数定 failed/partial）；恢复时点取最后一个 done 段的备份时点。清理抢先删段导致
   链截短、恢复时点早于目标时，接口显式给 `short_of_target=true`，页面红字提示。

演练状态机：`queued → running → completed | partial | failed`，任意非终态可经
`interrupted` 由 `POST /api/drills/{id}/retry` 或 worker 自动续跑。

## HTTP API

| 方法/路径 | 说明 |
|---|---|
| `GET  /health` | 健康检查 |
| `POST /api/backups` | 登记备份（name/kind/taken_at/expires_at?/parent_id?/size_kb?） |
| `GET  /api/backups` | 备份清单（含 expired、pending_delete、in_use 活跃占用数） |
| `POST /api/drills` | 按目标时点发起演练，响应即含规划链与首个缺段 |
| `GET  /api/drills` / `GET /api/drills/{id}` | 演练队列/详情（步骤、恢复时点、缺段、占用数） |
| `POST /api/drills/{id}/retry` | 中断/失败/部分恢复后续跑，不新增占用 |
| `POST /api/cleanup` | 清理过期备份；返回已删与在用延后清单 |
| `GET  /api/events` | 审计事件 |
| `POST /api/test/pause` / `resume` / `crash` | 演练中断/重启的测试钩子 |

## 测试覆盖

| 文件 | 内容 |
|---|---|
| `tests/test_service.py` (16) | 链路规划、缺段口径、过期拦截、清理抢先截短、共用备份占用、重试不重复、清理后历史恢复时点保留、并发受理/清理、校验 |
| `tests/test_restart.py` (3) | 崩溃后新进程重开同一 DB：原链/进度保留、续跑、多次重启占用不增 |
| `tests/test_http.py` (7) | 真实 HTTP 端到端：完整流程、缺段、400、并发 45 请求、硬杀进程后换实例重启 |

## 边界

受控教学/运维演练模型：只模拟备份元数据与恢复链计算，不接触真实备份数据、不执行真实还原；
时间口径为 UTC ISO8601 内部存储，前端按浏览器本地时区展示。
