# 独立复审修复与验证~

本轮处理复审确认的 6 个问题，原始正式样本、测量、旧交付和镜像保留原身份；新证据在 `data/review-fixes-20261030/`，新交付使用 `data/delivery-review-fixed-20261030/`~

## 修复行为~

- 验收脚本统一跟踪所有采样、验证、报告和回放进程，SIGINT/SIGTERM 只记录停止请求，在正常控制流中向子任务进程组转发信号，等待退出、必要时强杀回收，并保存 `complete=false`、错误和结束时间~
- 业务摘要、信号、WAL、水位及源码/二进制身份检查改成显式异常，`python -O` 不会跳过；微基准摘要校验同样保留~
- 所有并行子任务每 20ms 轮询，`finished_at` 表示父进程观察到该任务退出的时间，不再等待最慢任务完成后统一填写；操作系统精确退出时刻不作承诺~
- Git 命令退出状态确认成功后才记录提交号，没有提交则为 `null`~
- 微基准附件 `data/core-optimization-validation-v2/` 补入原始 5640 条 `input.wal`，SHA 为 `19bd5016a8cb6c235ff4539a70b58db10b134a27a0339bcdf996879f95df7c63`；保留原始报告副本和测量，便携报告只调整输入路径及增加来源标记，不伪称重新测量~
- 回放在初始化前注册同一组停止信号，初始与最终哈希改为异步分块读取；取消不留下后台全文件扫描，已进入计算链路的记录仍排空并保留失败摘要，初始源哈希未知时为 `null`~

## 验证范围~

34 项 Rust 测试与 6 项 Python 标准库测试全部通过，fmt、Clippy `-D warnings` 和 release 构建通过；新增测试覆盖普通/优化模式的不一致拒绝、各阶段及并行回放中断、独立结束时间、无提交身份、固定输入复制、启动两种信号与持久化开关组合、处理中和最终哈希取消、跨块哈希一致及挂起读取取消~

上一轮的 900 秒真实样本与 1/2/0 倍容量测试属于上一轮源码 SHA，本轮不重写其历史；本轮已用新二进制严格校验并全量回放该 WAL，并执行新版本短时真实闭环与验收脚本验证，不把它称为重新取得 900 秒样本~

旧 `acceptance.json` 中的 `git_commit="HEAD"` 无效，旧并行 `finished_at` 是顺序等待时观察到的时间；无法还原其精确历史退出时刻，原测量耗时仍以各目录 `summary.json.elapsed_ms` 为准；补充说明见 `data/review-fixes-20261030/historical-metadata-notes.json`，原文件不改写~

## 可复现命令~

```bash
cargo build --release --locked
cargo fmt --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
python -m unittest discover -s tests -p 'test_*.py' -v
python scripts/acceptance.py --out data/new-acceptance --seconds 20
python -O scripts/acceptance.py --out data/new-acceptance-optimized --seconds 3
```

验收输出目录必须不存在；正式新采样仍使用 `--seconds 900`，本轮短样本不替代上一轮正式样本，也不构成新版本完整性能验收~

## 实际结果与当前交付~

- 原生新二进制严格校验原正式样本通过；同一 WAL 的计算与持久化全量回放均为 388457 条记录、2126 条信号，Stats 和信号完全一致，持久化 WAL 与源文件逐字节一致，原始输入未改写~
- 原生串行计算回放耗时 2846ms，持久化回放耗时 3953ms，最大事件年龄分别为 80.511ms、116.927ms；当前 elapsed_ms 包含初始化哈希，不能与旧版排除初始哈希的总耗时直接当作算法性能比较~
- 首次在镜像构建并行期间的不限速计算回放触发 100ms 入队上限，程序返回非零并保存失败摘要；完整证据在 formal-compute/ 与 concurrent-replay-attempt.json，未放宽阈值；构建及其他测试结束后，同配置串行复核通过~
- 普通模式验收脚本完成新的20秒真实样本（3905条、1条信号）及全部回放，python -O 模式完成3秒短样本（690条、0信号）及全部回放；两者 git_commit 均为 null，并行回放结束时间分别及时记录，完成标记与所有对账均通过；优化模式不一致拒绝另由回归测试证明~
- 新镜像完成20秒真实容器样本（3853条、0信号）、禁网verify及持久化回放；另以新镜像禁网全量回放历史正式WAL，388457条、2126信号，Stats、WAL和信号全部一致，避免只证明空信号一致~
- 新微基准附件已实际打包/解压并验证输入 SHA、长度、CRC、序号、时间和5640条计数，历史原始报告字节保留；新脚本再次执行时会复制输入并保存相对路径，不虚构重测比例~

当前镜像为 `yino:review-fixed-20261030`，image ID 为 `sha256:84ca056719303c44d3b0fe156f50d40eaad3690ba082bc1056da175b157d0423`；归档 `data/yino-review-fixed-20261030.tar.gz` 为 30509230 字节，SHA256 为 `2f9b71b277ceafa036c3fdba15d83758857f06a4286e4646679eb2b5b2d7c219`，gzip 与实际 docker load 通过，最终镜像环境仅 PATH~

当前交付索引为 `data/delivery-review-fixed-20261030/INDEX.md`；历史900秒采样保留原源码SHA、错误Git/时间元数据补充说明、新版本回归证据和可移植微基准均随附件提供；本轮没有提交、推送或公开发布~
