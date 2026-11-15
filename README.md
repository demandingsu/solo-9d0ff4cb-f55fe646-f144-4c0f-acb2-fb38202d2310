# 野外水质探头 · 时钟回拨校时

野外探头设备时钟回拨后，用核时锚点把读数线性校正到 UTC，按 UTC 日整版发布
CSV。原始读数只读；每次补交锚点生成一个不可变的新发布版本，旧版本链接永远
返回原文件。

## 运行（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000 ，点击“载入演示样例”
```

数据（SQLite、原始 CSV、已发布目录）持久化在命名卷 `wq-data` 里。
部署固定为**单 worker**：发布协议（分配修订号 → 原子 rename → 登记 SQLite）
依赖进程级锁，SQLite(WAL) 是唯一的共享状态。

## 本地运行与测试

```bash
pip install -r requirements-dev.txt
uvicorn app.main:app --reload          # http://127.0.0.1:8000
python -m pytest                       # 全部回归测试
```

## 并发读一致性设计

一次补交 = 一个 SQLite 事务：插入锚点 → 暂存并原子 rename 新目录 → 登记发布
行 → 提交。失败或冲突整体回滚，旧版保持可见。

一次页面渲染 = 一个 SQLite 读事务（`Store.snapshot`，见
`Service.state()`）：分段校时结果、待核读数、锚点、当前版本和文件目录全部
来自**同一次已提交的发布**。同事刷新赶上资料员补交时，WAL 快照隔离保证页面
只会是完整旧版或完整新版，绝不会把旧版的 24 条待核序号和新版“待核 0 条”的
日文件链接混在同一响应里。下载解析（`Service.file_path`）同样走单快照。

回归验证（`tests/`）：

- `test_read_consistency.py` — 服务级：用 `Service.state_read_hook` 把一次
  页面读精确停在“已读锚点、未读发布行”之间并在此窗口提交补交，断言响应是
  完整旧版；另有并发压测（4 读线程 × 连续补交，逐次断言不变量）、补交失败
  仍显示旧版、旧版链接字节不变。
- `test_http_concurrency.py` — 端到端：真实 uvicorn 服务上并发 GET/POST，
  对每次响应断言页面内部一致性（版本号、待核数、待核序号标签、锚点标签、
  当前目录链接指向同一发布 id）。
