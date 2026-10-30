# YINO 实时行情 Demo~

单进程 Rust 系统：公开 BTCUSDT WebSocket → 有界队列 → WAL 批量同步 → 有界队列 → 整数因子与策略 → 信号输出；实时与回放共用计算引擎，提供检查点恢复验证和性能报告~

按优先级的修复任务见 [TASKS.md](TASKS.md)，独立复审修复和当前交付见 [REVIEW_FIXES.md](docs/REVIEW_FIXES.md)，上一轮正式采样与容量证据见 [ACCEPTANCE.md](docs/ACCEPTANCE.md)；[最小闭环记录](docs/MINIMAL_LOOP.md) 和 [重连记录](docs/RECONNECT.md) 是历史阶段证据，其中镜像及摘要不代表当前版本~

## 原生运行~

需要 Rust 1.95.0 和 Linux；工具链与依赖由 `rust-toolchain.toml`、`Cargo.lock` 固定，资源统计读取 Linux `/proc`；所有 `--out` 目录必须不存在，防止覆盖原采样或回放~

```bash
cargo build --release --locked

# 短时连通性与闭环检查。
./target/release/yino live --out data/short/live --seconds 60
./target/release/yino verify --input data/short/live
./target/release/yino replay --input data/short/live/raw.wal --out data/short/replay --speed 0
./target/release/yino report --input data/short/live

# 正式采样入口；默认 --seconds 也是 900。
./target/release/yino live --out data/formal/live --seconds 900
./target/release/yino verify --input data/formal/live
./target/release/yino report --input data/formal/live
```

快速闭环脚本为 `bash scripts/smoke.sh data/smoke 60`，会执行采样、回放、严格校验和信号文件逐字节比较；示例目录重复使用前需另选新目录~

默认数据源为无需密钥的 `data-stream.binance.vision` combined stream，订阅 `btcusdt@trade` 与 `btcusdt@bookTicker`；`live --endpoint ws://...` 可接入本地故障测试服务~

`complete=true` 只表示本次指定运行通过相应检查，不等于正式验收；正式交付还需要有效的 900 秒真实样本、严格 `verify`、检查点验证、全链路容量和故障实验，实际结果以验收记录为准~

## 回放与容量测试~

`--speed 1 / 2 / 10` 分别为原速、2 倍速和 10 倍速，`0` 表示不限速；注入保留原始突发间隔，业务计算始终使用 WAL 中的逻辑时间，性能统计使用本次运行的注入时间~

```bash
# 只回放计算，不生成新的 raw.wal。
./target/release/yino replay --input data/formal/live/raw.wal --out data/formal/compute --speed 0

# 与实时共用两级队列、持久化和计算线程，写入新的 WAL。
./target/release/yino replay --input data/formal/live/raw.wal --out data/formal/persist-1x --speed 1 --persist
./target/release/yino replay --input data/formal/live/raw.wal --out data/formal/persist-2x --speed 2 --persist
./target/release/yino replay --input data/formal/live/raw.wal --out data/formal/persist-max --speed 0 --persist
./target/release/yino report --input data/formal/persist-2x
cmp data/formal/live/signals.jsonl data/formal/persist-2x/signals.jsonl
```

`--persist` 保留源文件，成功回放要求新 WAL 与源 WAL 的 SHA-256 一致；摘要记录 `source_wal_sha256`、`replay_speed` 和 `mode=replay_persist`，未持久化的回放不能作为全链路容量证据；`verify` 的正常验收入口只接受实时样本~

## 配置、重连与停止~

以下参数同时适用于 `live` 和 `replay`，配置在运行前固定并写入 `summary.json`~

| 参数 | 默认值 | 用途 |
|---|---:|---|
| `--queue-capacity` | 8192 | 两级队列各自的容量~ |
| `--batch-size` | 1024 | WAL 单批最多事件数~ |
| `--batch-ms` | 5 | 从首条出队事件起算的批次等待期限~ |
| `--enqueue-timeout-ms` | 100 | 单次入队最大等待时间~ |
| `--max-event-age-ms` | 1000 | 计算开始时的本地事件年龄上限~ |
| `--idle-timeout-ms` | 10000 | 实时行情静默检测与各数据流最大空窗检查阈值~ |

持久化线程先刷新缓冲区，再执行 `sync_data()`，成功后才交付整个批次；入队超时或写盘失败会停止链路并返回非零，已同步但未计算的记录留在 WAL 中供恢复，不静默丢弃后宣称成功~

连接持续无文本行情达到 `--idle-timeout-ms` 时记录数据缺口并重连，Ping/Pong 不刷新行情活动期限；结束时还独立检查成交和报价各自的最大空窗，避免单个数据流持续静默被另一条流掩盖~

建连每轮最多尝试 9 次，单次超时 15 秒，重试按 250ms 起指数退避至最多 5 秒；采样从首次成功连接开始计时，重连不延长截止时间；断线后清空窗口与报价，保留成交去重水位和目标方向，等待新报价及 1000 笔有效新成交重新预热~

实时及所有倍速回放统一响应 SIGINT/SIGTERM，回放在创建输出目录前注册信号，初始与最终文件哈希也可中断；初始哈希未完成时 `source_wal_sha256=null`，明确表示没有完成源文件校验；中断会停止接收或注入、取消建连及队列等待，再排空已进入链路的记录、保存检查点和失败摘要，返回非零；发生过连接缺口的样本即使重连成功也保持 `complete=false`~

## 检查点与恢复~

计算线程每秒及正常结束保存带 CRC32 的检查点，写入前先刷新并同步配套信号文件；检查点记录最后处理序号、结果和信号数量、累计业务摘要、信号摘要及字节水位~

```bash
# 检查异常样本的完整 WAL 前缀及可证明的实时处理前缀；预期非零退出。
./target/release/yino verify --input data/interrupted/live --recover

# 对损坏 WAL 的有效前缀生成离线结果；预期非零退出。
./target/release/yino replay --input data/broken.wal --out data/recovery --recover
```

`verify --recover` 返回验证范围、`prefix_proven` 和可用检查点；只比较 WAL 重算摘要及完整信号前缀都通过的检查点，损坏或截断的检查点尾部不会被采用；没有可用检查点时只报告 WAL 完整前缀，不宣称实时一致性；恢复命令始终非零，不能代替正常验收~

## 输出、校验和报告~

| 文件 | 内容 |
|---|---|
| `raw.wal` | 原始文本消息及有序连接控制事件，含连续序号、连接编号、UTC 纳秒与单调时钟偏移；计算回放不生成此文件~ |
| `signals.jsonl` | 目标变化信号，包含逻辑时间、整数因子和方向~ |
| `checkpoints.jsonl` | 带校验和的累计结果与信号检查点~ |
| `system.jsonl` | 连接、心跳、状态变化及失败日志~ |
| `resources.jsonl` | 按秒记录 CPU、RSS、进程磁盘字节、队列水位及接收/处理速率，结束时追加最终记录~ |
| `summary.json` | 配置、业务摘要、文件摘要、各阶段延迟、活跃度、积压与排空耗时~ |
| `report.md` | `report --input` 根据摘要生成的可阅读性能和验收报告~ |

WAL 版本为 `YINOWAL1`，每条记录由小端 u32 长度、小端 u32 CRC32 和 JSON 载荷组成，载荷最多 1 MiB；读取检查长度、校验和、连续序号、单调时间和连接状态，首个损坏处立即停止，不跳过异常继续读取~

`verify` 重新读取完整 WAL，核对事件分类计数、业务与信号摘要、接收/同步/处理水位、最终检查点、各流活跃度和本地事件年龄；任何差异返回非零；当前 `summary.json` 使用 `format_version=2`，不兼容旧摘要，旧 `YINOWAL1` 仍可用当前程序回放生成新格式结果~

延迟报告提供 P50/P95/P99/最大值，分别测量两级入队与队内等待、批次等待、缓冲刷新、独立 `sync_data()`、持久化、引擎计算、事件年龄、接收至决策和信号刷新；批次大小单位为条，信号刷新不等于逐条同步磁盘；消费者早于发送确认的队内等待无法观测，单独计数，不填零，全量端到端统计仍覆盖所有事件~

## 测试与交付打包~

```bash
cargo fmt --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
python -m unittest discover -s tests -p 'test_*.py' -v

# 源码、文档、指定采样及 SHA-256 清单，输出目录必须为新目录。
bash scripts/package.sh data/delivery data/formal/live data/formal/persist-2x data/core-optimization-validation-v2
```

测试覆盖手算信号、报价新鲜度和价差精确边界、不同成交量的窗口参照复算、去重、异常数值、重连预热、SIGINT/SIGTERM、静默检测、慢同步、两级队列饱和、同步失败、WAL 损坏及检查点恢复；优化微基准入口为 `python scripts/benchmark_core.py --help`，其结果不能替代全链路容量测试~

`data/core-optimization-validation-v2/` 包含原微基准输入、前后源码、原始测量及相对路径说明，复现命令见附件 README；新微基准会复制固定输入到自己的输出目录，普通和 `python -O` 下均执行摘要校验~

自动验收入口为 `python scripts/acceptance.py --out data/new-acceptance --seconds 900`；验收脚本在普通与优化模式均执行显式一致性检查，SIGINT/SIGTERM 会停止并回收所有子任务，失败状态写入 `acceptance.json`；并行任务各自记录观察到退出的时间，轮询间隔 20ms；无提交仓库的 `git_commit=null`~

打包脚本归档源码与指定样本，生成文件及归档 SHA-256 清单，不执行提交、推送或发布；镜像获取方式、镜像 digest、正式样本和最终验收结论统一记录在 [ACCEPTANCE.md](docs/ACCEPTANCE.md)~

## Docker 运行~

镜像从源码和同一锁文件多阶段构建，通过挂载目录保存全部结果；容器采用相同命令行入口和 SIGTERM 收尾处理~

```bash
docker build -t yino:current .
mkdir -p data
docker run --rm -v "$PWD/data:/data" yino:current live --out /data/docker-short --seconds 60
docker run --rm -v "$PWD/data:/data" yino:current verify --input /data/docker-short
docker run --rm -v "$PWD/data:/data" yino:current live --out /data/docker-formal --seconds 900
docker run --rm -v "$PWD/data:/data" yino:current verify --input /data/docker-formal
docker run --rm -v "$PWD/data:/data" yino:current replay --input /data/docker-formal/raw.wal --out /data/docker-persist-2x --speed 2 --persist
docker run --rm -v "$PWD/data:/data" yino:current report --input /data/docker-formal
```

原生和 Docker 的 900 秒命令均为复现入口，示例命令本身不构成已通过证据；本轮实际运行结果以验收记录为准~
