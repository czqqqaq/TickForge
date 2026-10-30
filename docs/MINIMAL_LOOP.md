# 最小闭环验证记录~

验证日期为 2026-10-28，已跑通真实 BTCUSDT 行情接收、同步 WAL、因子与目标方向计算、信号写出及确定性回放；这是 PLAN 第一阶段的短时验证，正式 900 秒采样和完整性能验收尚未完成~

## 实际结果~

| 项目 | 原生运行 | Docker 运行 |
|---|---|---|
| 指定采样时长 | 60 秒 | 30 秒 |
| 接收 / 同步 / 处理记录数 | 5640 / 5640 / 5640 | 2566 / 2566 / 2566 |
| 成交 / 报价 / 控制事件数 | 1110 / 4528 / 2 | 746 / 1818 / 2 |
| 异常 / 重复事件数 | 0 / 0 | 0 / 0 |
| 交易信号数 | 2，依次为 LONG、FLAT | 0，未满 1000 笔成交预热门槛 |
| WAL 批次同步次数 | 85 | 52 |
| 持久化延迟 P99 | 963.583ms | 113.279ms |
| 接收至处理完成延迟 P99 | 964.095ms | 113.791ms |
| 处理延迟 P99，包含摘要与信号写出 | 31μs | 30μs |
| 不限速回放 | 全部业务统计、结果摘要和信号文件一致 | 全部业务统计、结果摘要和信号文件一致 |
| `verify` | 通过 | 通过 |

以上性能仅为共享宿主机短时观察，原生采样时还在并行构建镜像；本轮同步尾延迟明显高于计算耗时，正式性能结论留待 PLAN 后续独立采样与容量实验~

原生样本另完成 10 倍速回放，5640 条结果及两条信号保持完全一致；Docker 镜像在禁用网络的容器中独立执行原生样本的 `verify`，得到相同业务摘要~

原生业务摘要为 `6f728ed385755980626c52b9b8efd98e26727d90db0d23f35c53d48da4080b08`，Docker 样本业务摘要为 `540c9821cfc4bc0befe882ca3bcaf3ac33c9a2947435c18d772a4baa8d21b4af`~

## 证据位置与复现~

以下路径相对于仓库根目录，运行数据保留在本机 `data/`，该目录由 Git 忽略~

- `data/native-smoke-20261028-tls12/live/`：原生 WAL、两条信号、结构化日志与完整统计~
- `data/native-smoke-20261028-tls12/replay/`、`replay-10x/`：不限速及 10 倍速回放结果~
- `data/docker-smoke-20261028/live/`、`replay/`：容器挂载目录采样与回放结果~
- `data/recovery-smoke-20261028/`：截断 WAL、严格模式及恢复模式的输出、退出码和错误证据~
- `data/acceptance-20261028/source-sha256.json`、`image.json`：实现文件与依赖锁文件摘要、镜像及归档标识~

```bash
./target/release/yino verify --input data/native-smoke-20261028-tls12/live
./target/release/yino verify --input data/docker-smoke-20261028/live
./target/release/yino report --input data/native-smoke-20261028-tls12/live
# 使用新的输出目录执行下一次短时闭环。
bash scripts/smoke.sh data/next-smoke 60
```

## 检查与故障证据~

`cargo fmt --check`、`cargo test --locked` 与 `cargo clippy --locked --all-targets -- -D warnings` 均通过，测试包含 4 个单元测试和 2 个完整进程的本地 WebSocket 集成测试~

同步失败测试使用允许写入但拒绝 `sync_data` 的文件描述符，确认持久化计数为零且计算队列没有收到事件；集成测试验证 Ping/Pong、实际信号、回放一致性、篡改信号被拒绝和提前断线的失败退出~

从真实 WAL 尾部删除 17 字节后，严格回放返回退出码 1，报错位置为字节偏移 1724253；恢复模式只处理前 5639 条完整记录，生成 `complete=false` 的摘要并同样返回退出码 1，不作为实时前缀一致性证明~

首次真实 TLS 验证发现 Rustls 未选择加密后端，补齐 `ring` 后又确认需启用 `tls12` 才能完成当前端点握手；最终依赖明确启用两者，未添加备用 TLS 实现，失败目录保留在 `data/native-smoke-20261028/`、`data/native-smoke-20261028-ring/` 和 `data/native-endpoint-probe/`~

## Docker 交付~

镜像由 Dockerfile 从源码及锁文件多阶段构建，运行镜像大小为 80,105,996 字节，平台为 `linux/amd64`；本地镜像名为 `yino:minimal`，镜像 ID 为 `sha256:c9e94fef75ee63ff3d949fe422ac1d01e88e662e31a0adff60503d40d828013f`，尚未推送到镜像仓库~

归档为 `data/yino-minimal-20261028.tar.gz`，大小为 30,538,213 字节，SHA-256 为 `f94bf1b8de8a1704abcdabd30a4e788d2934409b2e667fc78ebadf2c35e8dd11`；已通过 gzip 完整性检查和实际 `docker load`~

```bash
docker load -i data/yino-minimal-20261028.tar.gz
docker run --rm -v "$PWD/data:/data" yino:minimal verify --input /data/native-smoke-20261028-tls12/live
```

本机原先没有 Docker，验证使用 `/tmp/yino-docker-tools/docker/` 下的独立工具和临时 Unix socket，不修改全局 Docker 配置；临时服务使用 vfs 存储及 host 网络运行采样，离线回放与校验使用禁网容器；构建代理只通过构建参数传入，最终镜像环境不包含代理配置；验证结束后已停止临时服务，镜像归档及所有采样文件保留~

本机 Cargo 镜像存在排队，构建时从 `/tmp` 运行 `CARGO_HOME=/tmp/yino-cargo cargo +1.95.0 ... --manifest-path /root/workspace/yino_test/Cargo.toml`，避开全局镜像配置；源码与依赖锁文件保持正常 Cargo 项目结构~

## 后续阶段~

自动重连、周期校验检查点、含持久化的回放容量测试、分段排队/资源指标、完整故障矩阵和正式 15 分钟样本继续按 PLAN 实施；当前代码尚未提交或推送 GitHub，未发布任何远端服务~
