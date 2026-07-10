# rustfs-uring 设计:io_uring 取消安全读后端(backlog#894 / #1048 / #1051)

> 本文档源自 Spike 0 取消安全原型(原 `SPIKE.md`),经 rustfs/backlog#1051 审计整改后作为 `rustfs-uring` 库的设计与不变量说明保留。逐 issue 修复的历史见本仓库 git log。

## 这是什么

rustfs/backlog#897 路线图中 P2(io_uring 读后端)被 P1.5 基准判 NO-GO 而 defer。本 spike 是 #894 明确要求先行的**取消安全原型**——P2 中风险最高、最容易随时间流失的知识,按"只实现原型、不进主干、不启用"的方案 B 存档。

> **现状更新(2026-07):** P2 主体已在此库实现并接入 `rustfs/rustfs`,但**默认灰度关**(`RUSTFS_IO_URING_READ_ENABLE`)。端到端 A/B(rustfs/backlog#1159)显示 io_uring 对 S3 GET 大致中性(−7%~+4%),瓶颈在用户态拷贝而非磁盘读,因此 #1048 转为 **[Watch] 看护 issue**——真实直连 NVMe 证据满足前不默认启用。下方"对 P2 主体实现的遗留项"一节记录了每项的落地/决策现状。本文其余部分(所有权模型、不变量、测试矩阵)是这些实现共同遵守的契约,持续有效。

**本 crate 是独立 workspace**(Cargo.toml 内含空 `[workspace]` 表),io-uring 依赖不进入 rustfs 主 Cargo.lock、不参与主工程构建与 CI。这与守卫脚本 `scripts/check_no_tokio_io_uring.sh` 的约束一致:禁的是 tokio 的 io-uring runtime feature,应用层显式 io-uring 集成必须走运行时探测的独立后端(即本原型验证的模型)。

## 要证明的问题

EC 读会 drop 在途的分片读 future;若该 future 已向内核提交了 read SQE,内核在 CQE 之前始终可能向目标 buffer 写入。**future 的 drop 不能回收 buffer,否则是 use-after-free。**

> **该场景在生产 GET 上被真实行使(2026-07 核对 main):** 主触发点是**读者建立阶段**——`crates/ecstore/src/set_disk/core/io_primitives.rs` 的 `create_bitrot_readers_until_quorum_all_shards`(`FuturesUnordered` 于 :1362 建立、setup quorum break 于 :1403、多余任务于 :1428 `drop(reader_tasks)`)。数据分片在此阶段经 `read_file_mmap_copy → UringBackend::pread_bytes → driver.read_at(...).await` **急切**读入,因此一个仍停在该 `.await` 的任务被 drop 时,其 `ReadHandle` 在 `Submitted` 态被 drop、触发 `ASYNC_CANCEL`(`ReadHandle::drop`,`src/driver.rs`)。这在**每个响应盘多于 setup quorum 的 GET(常态)**都发生,与后续是否重建无关。decode 阶段(`ParallelReader`,`crates/ecstore/src/erasure/coding/decode.rs`)的 `FuturesUnordered` 也在 quorum 处 drop 落败者,但对 io_uring 只在**非 lockstep 路径的延迟 parity reader 边打开边被 drop**时命中(数据分片此时已是内存 `Bytes`,lockstep 路径则全量 drain、从不中途 drop)。

## 验证的所有权模型

```
caller                    driver thread                     kernel
------                    -------------                     ------
read_at() ──Msg::Read──▶  分配 buf,登记 pending 表
                          (buf + Arc<File> + oneshot tx)
                          push SQE(user_data=id) ─submit──▶ 开始随时可能写 buf
await ◀───oneshot────────                                    │
                                                             │
future drop(任意时刻)                                        │
  └─(可选)Msg::Cancel ──▶ push ASYNC_CANCEL ────────────────▶│ 加速 CQE
  └─绝不触碰 buf                                             │
                          CQE 到达 ◀─────────────────────────┘
                          pending.remove(id)  ← 全程唯一的 buf 回收点
                          send 结果:成功=delivered
                                    失败(接收方已 drop)=orphan_reclaimed
```

关键不变量:

1. **buffer 与 fd 归 pending 表所有,不归 future。** SQE 里的裸指针指向表项 `Vec` 的堆块;`Vec` 结构体可随 HashMap 移动(堆块地址不变),但在 CQE 前绝不 resize/drop。
2. **fd 也必须由表项持有**(`Arc<File>`)。真正的危险窗口是 **SQE 构造(`as_raw_fd`)→ `io_uring_enter` 内核消费**:此窗口内 SQE 携带裸 fd 号在 backlog 中滞留、内核尚未 `fget`;若 fd 被 drop 关闭并被新 `open` 复用,提交时内核解析到错误文件——对 READ op 意味着从**错误文件读出数据**(跨对象数据错读/泄露),而非"内核的写落到别人文件"。表项持有 `Arc<File>` 到 CQE 是该窗口的安全超集。(机理更正见 rustfs/backlog#1063:原文把危险窗口误标为"提交→CQE"、后果误标为"写别人文件"——已提交的 op 因内核已持 struct file 引用而对 fd close/复用免疫;若未来用 SQPOLL,消费点还会与 enter 脱钩。)
3. **future drop 只放弃结果领取**,默认附带提交 `IORING_OP_ASYNC_CANCEL`(best-effort 加速),也可以不提交(裸 drop)——两种情况下回收都只发生在 CQE。
4. **shutdown 顺序**:停收新 SQE → 对所有在途 op 提交 cancel → drain 到 `in_flight == 0` → 线程退出 → ring drop(unmap)。ring 决不能在内核仍持有 buffer 引用时 unmap。
5. **探测必须提交真实 read op**:`io_uring_setup` 成功不代表 op 可用(gVisor/seccomp 可以建 ring 但 op ENOSYS/EINVAL);探测失败按 EACCES/EPERM/ENOSYS/EINVAL/EOPNOTSUPP 分类,命中即优雅降级(测试中表现为 skip),其余 errno 视为真 bug 直接断言失败。

以下不变量是本次审计整改(rustfs/backlog#1051)新增/固化,P2 必须一并沿用:

6. **驱动线程 unwind 安全**(rustfs/backlog#1054):驱动线程绝不允许在栈展开中释放 pending 表或 unmap ring——否则内核仍可能向在途 buffer 写入即 UAF。实现为 `DriverState::Drop` 检测 `thread::panicking()` 时在字段析构前 `process::abort()`(leak over UAF)。所有 caller 可控的 panic 面(如超大 `len`)在 `submit` 入口拒止。`catch_unwind` 不够——析构在展开时、catch 边界之前就已发生。
7. **背压 permit 在 CQE 点释放**(rustfs/backlog#1060;异步化见 #1102):in-flight 上界 ≤ CQ 容量(取 SQ 深度 `entries` < `2*entries`,使 CQ overflow 结构性不可达),permit 随 pending 表项移除(CQE)释放,**绝不随 future drop 释放**——否则 quorum 大量 drop future 会让 permit 计数与驻留内存脱钩,重开内存 DoS 面。
   - **已落地**:`tokio::sync::Semaphore`;`OwnedSemaphorePermit` 随 `Msg::Read` 存进 `Pending` 表项,表项在最终 CQE 被移除时 permit 自动 drop ——"CQE 点释放"由**类型系统强制**,不再依赖手写 `release()`(短读 resubmit 保留表项,故也保留 permit)。
   - **获取从不阻塞线程**:未饱和走 `try_acquire_owned()` 快路径(无分配、无 await、提交仍是即时的);饱和时把 acquire future 交给返回的 `ReadHandle`,首次 poll 时 await 到 permit 再提交。因此**一个在首次 poll 前就被 drop 的 handle 从未提交、从未分配 buffer**(比阻塞实现更省内存),`delivered + orphan_reclaimed == submitted` 的守恒式仍恒成立。
8. **复用缓冲内容卫生**(rustfs/backlog#1062,P3 前置):当前 spike 每 op 新分配零页 + `truncate(res)`,**无泄露**。P3 改用驱动自有对齐 slab(registered buffer)后,缓冲跨请求复用即脏内存——任何路径忘记按 `cqe.res` 截断/掩蔽(O_DIRECT 整块读再由上层切片、或错误路径把整块缓冲交还)就把上一租户请求的对象字节泄给当前请求(CWE-226)。不变量:**复用缓冲对调用方可见的字节严格 ⊆ `[0, cqe.res)`**,越界部分零化或由类型系统(返回带长度上限的 view 而非整块 slice)保证不可达。docs/DESIGN.md 与 #1048 原约束只讲 slab 生命周期(防 UAF),不讲内容卫生;需配套"脏缓冲 + 短读"回归测试。

补充契约:

- **errno 三分类**(rustfs/backlog#1059):`is_expected_restriction` **仅用于 probe 期**;运行期 errno 必须分——probe 受限 → 该盘永久降级;运行期参数错误(offset>i64::MAX、O_DIRECT 未对齐等 EINVAL)→ 返回错误、绝不闩锁;瞬态(EINTR/EAGAIN)→ 重试。`submit` 已在入口拒止 offset>i64::MAX 与 len>MAX_RW_COUNT。
- **shutdown 有界 drain**(rustfs/backlog#1055):drain-to-zero 可能因坏盘上 cancel 不可中断(EALREADY)而不终止;超时(`DRAIN_TIMEOUT`)后泄漏 ring+buffer 退出(leak over UAF),绝不提前 unmap。cancel CQE 三态(succeeded/not_found/already)已纳入统计,EALREADY 上升即坏盘信号。
- **短读 resubmit**(rustfs/backlog#1058):io_uring 对常规文件可合法短读;驱动 resubmit 剩余到 `buf[nread..]`,回收点移到逻辑读的最后一个 CQE。P2 须明确短读归属(后端循环 vs 调用方 `read_exact`)。

## 测试矩阵

| 测试 | 验证点 |
|---|---|
| `read_matches_std` | 完成路径正确性:64 次变长/变偏移读与文件内容逐字节一致 |
| `dropped_future_buffer_lives_until_cqe` | **核心断言**:阻塞的 pipe 读上裸 drop future(不提交 cancel),300ms 后 op 仍 in-flight、buffer 未回收;向 pipe 写入触发 CQE 后才回收(orphan_reclaimed=1) |
| `async_cancel_accelerates_reclaim` | 默认 drop 路径:ASYNC_CANCEL 使孤儿 op 在无数据到达的情况下经 ECANCELED CQE 及时回收 |
| `cancel_stress_accounts_for_every_buffer` | 压力:256 并发读、一半立即 drop;`delivered + orphan_reclaimed == submitted`,幸存读逐字节正确 |
| `shutdown_drains_in_flight_ops` | 关停:两个阻塞在途 op 被 cancel + drain 到 0 后线程才退出,持有的 future 解析为 ECANCELED |

> 上表是 spike 原始 5 项核心断言。接入期实现随之新增覆盖,`tests/cancel.rs` 现共 **15 项**,补充:`saturated_submit_defers_instead_of_blocking`(异步背压不阻塞 runtime worker)、`direct_read_returns_exact_unaligned_ranges`(O_DIRECT 非对齐区间精确交付、填充不外泄)、`sharded_driver_conserves_buffers_across_shards` 与 `sharded_driver_with_one_shard_matches_single_ring`(分片下守恒 + 单片等价)、以及 boundary/pipe/EOF 等边界。所有项在 `run-docker.sh` 两腿下运行(leg 1 全优雅降级、leg 2 真实 io_uring)。

需要 Docker(Linux 内核)。macOS 宿主上 `cargo check` 只验证非 Linux 桩编译。

```bash
./run-docker.sh
```

- **leg 1(默认 seccomp)**:多数 Docker 版本默认禁 io_uring(即 #4313 事故环境),探测失败 → 全部测试走优雅降级 skip,套件仍绿。若宿主 Docker 放行 io_uring,则此腿等同 leg 2。
- **leg 2(seccomp=unconfined)**:真实 io_uring,完整跑取消安全套件。

## 运行结果

两腿一次通过,详见"实测记录"。

## 对 P2 主体实现的遗留项 — 落地/决策现状

> 本节原为"本 spike 不覆盖"的清单;下列各项经 rustfs/backlog#1102/#1144/#1145/#1159 处理后现状如下。三种收尾:**✅ 已实现**、**⛔ 经度量/设计决策关闭(不做)**、**⬜ 仍未做(正确 defer)**。

- **✅ eventfd 唤醒收割(rustfs/backlog#1102)。** 200µs 忙轮询已由 eventfd 替换:一个 eventfd 注册到 ring(内核每 CQE 信号)、一个由 `submit`/shutdown 信号,驱动线程 `poll` 两者阻塞等待,`submit()` 每轮仍冲刷 NODROP overflow list。
  - **⛔ tokio `AsyncFd` 去驱动线程 — 不做。** `Drop` 不能 `await`,shutdown 的有界 drain 排空会被逼成公开 API 破坏。现"专用驱动线程 + eventfd 阻塞收割"已消除忙轮询,是无破坏的等价收益。
- **✅→⛔ ring 生命周期 → 改为 per-disk 分片。** "进程级单例 ring"与坏盘隔离诉求(一块坏盘不得拖垮其它盘)相冲,已**重定义**为 per-disk ring 集,并用 `probe_and_start_sharded(entries, shards)` 在盘内横向扩展(每分片独立线程/pending 表/背压/eventfd)。Drop/shutdown 先让所有分片停再逐片 join,`DRAIN_TIMEOUT` 上界防坏盘无界阻塞。
- **✅ O_DIRECT 对齐读:`read_at_direct(file, offset, len, align)`(rustfs/backlog#1102)。** 驱动读**块对齐超范围**到**块对齐 buffer**(超额分配 `align-1` 字节,在分配内部取第一个对齐字节作为读区起点),完成后只把逻辑区间 `[offset, offset+len)` 切出——**对齐填充、区间前缀、块对齐尾部一律不外泄**(否则 `BitrotReader` 会把补齐字节当损坏)。短读 resubmit 保持块对齐;内核返回非块倍数即文件尾。缓冲读是 `align == 1` 的退化情形。
  - **✅ ecstore 已原生接线**(rustfs/rustfs#4649):`pread_uring_direct` 以 O_DIRECT 打开 fd 并调 `read_at_direct`,分层兜底 + per-disk 闩锁。**已取代 #4645 的临时分流**(那版把 O_DIRECT 合格读交回 StdBackend)。
- **✅→⛔ 三读形态接入 `LocalIoBackend` — 部分做、部分不做。** **定位读 `pread_bytes` 已接** io_uring(缓冲 + 原生 O_DIRECT)。**流式读 `open_read_stream`/`open_full_read` 判 NO-GO**(rustfs/backlog#1144):io_uring 对单条顺序流无杠杆(内核 readahead 已胜),冷读设备瓶颈、暖读仅 11–41% 于 buffered——保持委托 StdBackend。
- **✅ per-disk 探测缓存 + 运行期 errno 降级闩锁(rustfs/backlog#1101)。** `URING_UNSUPPORTED_DISKS` 负探测缓存 + `is_io_uring_unsupported` 运行期 errno 三分类(仅限制类 `EPERM/EACCES/ENOSYS/EOPNOTSUPP` 触发整盘闩死,数据/参数错误不闩),与 probe 期分类分离。
- **⬜ registered buffers(P3)/ 写路径(P4)— 仍未做(正确 defer)。** `register_buffers` 与本文档"buffer 归 pending 表、`Vec<u8>` 所有权"的取消安全模型冲突,需重设计;且 #1159 端到端 profiling 显示读路径已非瓶颈,优先级低。写路径(PUT)完全未涉及,profiling 提示其收益可能大于读路径。

**已在本 spike 内整改**(rustfs/backlog#1051):SQ 深度背压(不变量 7)、驱动线程 unwind 安全(不变量 6)、shutdown 有界 drain、CQ overflow/NODROP/EBUSY 处理、probe UAF、probe 文件安全创建、errno 三分类、len/offset 校验、短读 resubmit。

**接入后新增的行为契约**(实现期发现,非 spike 原文):`UringBackend` 读完须遵守 StdBackend 的页缓存回收策略(≥4 MiB 读后 `fadvise(DONTNEED)`),否则开启 io_uring 即静默污染页缓存(rustfs/rustfs#4662 修复的回归)。

## 实测记录

2026-07-07,宿主 macOS + OrbStack Docker(Linux arm64,内核 7.0.11-orbstack),镜像 `rust:1-bookworm`,`cargo test --release`:

- **leg 1(默认 seccomp)**:`io_uring_setup` 失败 `EPERM (Operation not permitted)`——与 #4313 事故环境同类。`ProbeFailure::is_expected_restriction()` 命中,5 个测试全部优雅降级 skip,套件绿。证明探测 + errno 分类降级契约按设计工作。
- **leg 2(seccomp=unconfined)**:5 个测试全部通过(0.45s):
  - `read_matches_std` ok — 64 次读逐字节正确;
  - `dropped_future_buffer_lives_until_cqe` ok — 裸 drop 后 op 保持 in-flight 300ms、buffer 未回收,写 pipe 触发 CQE 后 `orphan_reclaimed=1`;
  - `async_cancel_accelerates_reclaim` ok — ECANCELED CQE 路径回收;
  - `cancel_stress_accounts_for_every_buffer` ok — 256 op、128 drop,`delivered(128) + orphan_reclaimed(128) == submitted(256)`;
  - `shutdown_drains_in_flight_ops` ok — drain 到 0 后退出,持有 future 解析为 ECANCELED。

**结论:GO(模型可行)。** buffer/fd 归驱动 pending 表、CQE 唯一回收点、ASYNC_CANCEL 加速、shutdown drain 的组合在真实内核上成立,且降级契约在受限环境下按设计生效。**P2 主体已据此模型实现**(见上"落地/决策现状"),接入 `rustfs/rustfs` 后默认灰度关,15 项取消安全验收测试是这些实现共同遵守的回归门禁。
