# smart_sched v2 D5/D6 轮验收记录（2026-11-16，进行中）

承接 `Docs/smart-sched-v2-results.md`（D1–D4 时代：N1/N2 劣于 V97，
根因=底座缺 lag 携带与唤醒抢占）。本轮验证 D5（lag 携带 + 唤醒 kick，
`da84e5abb`）与 D6（detach 看门狗，`1ebdba7b5`）。

## 环境事故时间线（影响数据可用性，判定时必须考虑）

- 00:45Z 购买节点组（DB c8i.4xlarge spot + LG c8i.2xlarge spot），
  auto-release 4h 兜底。
- 01:33Z 为补 sync0rw-t 在基准进行中开启了 16 路单测编译 →
  **r1 SG/SN 与 r1 V97（部分）被污染，全部作废**（污染表现为 SG
  t64 只有 2,191 tps）。
- 02:05Z DB 节点被 spot 回收（rwscaling r1 V97 t256 中断）。
  本地结果目录已在重跑前删除，远端副本不可再取——**SN/SG r1 原始
  log 永久丢失**，仅剩 LG 侧抢救副本（见下）。教训已录入
  ENVIRONMENT.md。
- 02:30Z provision.sh 重买失败（ID 复用陷阱），手动 RunInstances
  重建（`i-0jl6a46yr3e5rc01knwd`，8.130.107.127 / 10.79.14.103）。
- 02:45Z 新节点双 mysqld 构建（源归档 48b8b0c18，含 D5/D6）。

## 1. D6 看门狗验证（通过）

| 实验 | 结果 |
|---|---|
| 挂死探测（`--watchdog-cmd 'sleep 30'`，interval 2s，timeout 1s，max-fail 2） | 2 次失败后 ~6s 内 detach：`sched_ext state` enabled→disabled，日志确认"watchdog detached … native EEVDF restored" |
| 退出码 | **3** ✓ |
| 健康负对照（`--watchdog-cmd 'true'`） | loader 持续存活不误杀 ✓ |
| 本地逻辑 harness（/tmp/wd_test） | 4/4：成功不 detach / 连败 detach / 挂死超时计败 / 间歇失败恢复 |

## 2. 单元与 smoke（通过）

- sync0rw-t（WITH_UNIT_TESTS 补编）：3/3 PASSED，含
  rw_lock_latch_wait_edges、sched_ext_lock_wait_mirror。
- smoke：SN 断连校验、SG attach+5 maps pinned、detach/reconnect、
  bridge 计数活跃、**wakeup_kicks=4**（D5 kick 路径确认激活）。

## 3. rwscaling 初步信号（被污染轮次剔除后的干净 r2）

**注意：以下数字来自被回收的旧节点，节点间波动大（SN r2 比 V97 r2
高 9–15%），仅作方向参考；正式验收以新节点完整重跑为准（进行中）。**

| 配置 | t32 | t64 | t128 | t256 |
|---|---:|---:|---:|---:|
| V97 r2 | 5,874 | 8,131 | 8,401 | 8,576 |
| SN r2 | 6,195 | 9,446 | 9,395 | 9,278 |
| SG r2（D5） | 7,333 | 8,829 | 8,623 | 8,507 |

方向性结论：**D5 后 SG 在干净负载不再系统性劣化**（上轮 SG 全档
-5~-8%），t32 甚至领先 V97 +25%（wakeup kick 对低并发闭环的锁传递
有效）；相对 SN 同水位。p95：SG t64 11.45ms，与 SN 11.16ms 同档
（上轮 SG 15.59ms vs SN 9.30ms）。

## 4. 待完成（新节点，按序）

1. setup→prepare→smoke（新节点首跑校验）
2. rwscaling 全量（V97/SN/SG/SS，多轮交错，零并行活动）
3. ratescan 含 **300k：验证丢唤醒卡死是否随 D5 消失**
4. N1（stress-ng 干扰）：验证 D5 是否把干扰期 p99 从 7.2ms 收敛回
   V97 的 0.2–0.6ms 量级
5. N2（混合负载）：验证 SG/SS mixed p99 是否从 123ms 收敛回 ≤V97
6. D6 全程挂 `--watchdog-cmd`（mysql SELECT 1）演练生产式运行
7. 若 3–5 通过：解除规划 D4 冻结的前提成立，再议自动触发

原始数据：`/root/src/aliyun/mysql-sched-bench/results/v2-20261116-salvaged/`
（LG 抢救副本，24 logs）+ 新节点结果目录（陆续生成）。

---

# 2026-11-17 复验轮：lag0 缺省覆盖 bug 的发现与修复

## 5. 环境与代码版本

- DB：`i-0jl0gubutiksaurmm1j5` c9i.4xlarge 16c **spot**（8.160.115.227 / 10.79.14.103）
- LG：`i-0jljdsf12p93lgcbwdkt` c9i.2xlarge 8c **spot**（8.160.182.143 / 10.79.14.104）
- auto-release 兜底 2026-11-17T08:00Z；结果先拉本地核验再释放
- 代码：`afb9c5334`（D5.1）+ `d224f0c50`（本轮 bug 修复）；双构建
  `/opt/mysql-sched-97`（V97 参考）与 `/opt/mysql-sched-smart`（SN/SG/SS）
- 四个 run 脚本均已加 ssh_retry 包装（5 次×15s，防公网 SSH 瞬断中断 campaign）

## 6. 关键 bug：`--lag-carry-us` 缺省被静默覆盖为 0（d224f0c50）

**现象**：D5.1 后的 campaign N1 复验中 SG noise p99 仍为 19.65/16.12ms
（V97 0.21/0.46ms），与 probe A/B 测得的 0.20ms 矛盾；且 kick 计数
（每阶段 ~5 万次）远低于 probe 的 40 万次/40s。

**取证**：DB 节点 `/tmp/scx_loader.log` 首行 `lag_carry=0us` —— campaign
实际以 lag carry = 0 运行。

**根因**（`support-files/scx/scx_db_sched_main.c`）：D5 引入
`--lag-carry-us` 选项时 `opt_lag_carry_us` 初值为 0，而 rodata 覆盖守卫
写成了 `opt_lag_carry_us >= 0`（恒真），未传参时 BPF 侧默认值
（D5.1 起 50ms，此前 5ms）总被覆盖成 0。对照 `opt_hog_slice_us`
的 `> 0` 守卫是正确写法。probe 当时**显式传了** `--lag-carry-us 50000`，
所以 probe 有效、campaign 无效。

**影响面**：D5 以来所有未显式传参的 SG/SS campaign（rwscaling、
ratescan、N1）实际都在 lag carry = 0 下运行；此前标称的 "lag5ms 旧默认"
从未在 campaign 中生效过。

**修复**：初值改 `-1` 哨兵，缺省保持 BPF rodata 默认。节点增量重编
（仅 scx_db_sched 目标，~1 分钟）后验证：缺省启动 echo
`lag_carry=50000us` ✓，SIGTERM 干净退出（state=disabled）✓。

**流程教训（已补 ENVIRONMENT.md 待办）**：campaign 前必须
`head -1 /tmp/scx_loader.log` 核验参数实际生效值；echo 行比 --help
可信（--help 显示的是默认值，不是本次运行值）。

## 7. lag0 消融数据（bug 期间的 SG/SS 数据归档为 `*-lag0`）

恰好构成"无 lag 携带"消融组（16c c9i，今日双轮均值）：

rwscaling（V97 → SG-lag0 偏差）：

| 档位 | V97 | SN | SG-lag0 | SG-lag0 vs V97 |
|---|---:|---:|---:|---:|
| t32 | 9,990 | 10,000 | 9,550 | **-4.4%** |
| t64 | 12,436 | 12,521 | 11,499 | **-7.5%** |
| t128 | 12,561 | 12,499 | 11,352 | **-9.6%** |
| t256 | 12,446 | 12,165 | 11,268 | **-9.4%** |

SS-lag0 与 SG-lag0 几乎相同（semantic 关闭时本就等价）。p95：
SG-lag0 t64 9.57ms vs V97 7.70ms；t128 25.97 vs 14.73。

ratescan：SG-lag0 250k=250,010、300k=**299,958**（无卡死，与 V97
300,044 同档）——注意：丢唤醒卡死的修复在 lag0 下就已成立，
说明修复来自 D5 的 kick/DSQ 排序路径本身而非 lag 携带。

N1（stress-ng 16 hogs 干扰）：V97 noise p99 0.21/0.46ms；
SG-lag0 19.65/16.12ms（kick 触发率低）。

N1 probe A/B（显式 `--lag-carry-us 50000`）：p99 **0.20ms**、
kick 40 万次/40s —— lag 携带对干扰场景的修复作用由此实证。

## 8. lag50（kick 无门槛）完整复验结果（2026-11-17 链）

lag50 真正生效后的完整两轮验收（16c c9i，SG 两轮均值）：

| 档位 | V97 | SN | SG-lag50-ungated | vs V97 | vs SG-lag0 |
|---|---:|---:|---:|---:|---:|
| t32 | 9,990 | 10,000 | 9,005 | -9.9% | -5.7% |
| t64 | 12,436 | 12,521 | 11,054 | -11.1% | -3.9% |
| t128 | 12,561 | 12,499 | 11,128 | -11.4% | -2.0% |
| t256 | 12,446 | 12,165 | 10,735 | -13.7% | -4.7% |

p95：SG-ungated 4.18/10.00/31.94/63.32 vs V97 4.26/7.70/14.73/30.27 ——
t64 起尾部明显劣化。

ratescan：SG 250k=250,120、300k=**300,017**（无卡死，lag50 下保持修复）。

**N1（stress-ng 16 hogs）：达标 ✓** —— SG noise p99 **0.21/0.23ms**
（V97 0.21/0.46；lag0 时 19.65/16.12），base 0.14/0.13。D5.1 的设计目标
（干扰期与原生 EEVDF 同档）完全达成。

**N2（FG rw64 + 8×BENCHMARK(SHA2) BG）：未达标 ✗**

| 配置 | solo tps / p99 | mixed tps / p99 | BG 完成数 | history list |
|---|---|---|---:|---|
| V97 | 12,307 / 9.73 | 9,915 / 16.71 | 183 | 362→252 |
| SN | 12,277 / 9.73 | 9,878 / 17.63 | 175 | 2027→913 |
| SG-ungated | 11,277 / 14.21 | 9,041 / **45.79** | 141 | 0→0 |
| SS-ungated | 11,237 / 14.46 | 9,448 / 43.39 | 150 | 0→0 |

mixed p99 45.79ms 仍劣于 V97 16.71（但远好于 D4 时代的 123ms）；BG
完成数 141 未饿死（同量级）；history list 无失控。N2 SG 阶段 kick 率
~10.6k/s（120s 内 128 万次）——**kick 同时落在 FG worker 身上**，既
伤吞吐也伤 mixed p99，这是 N2 与干净负载劣化的共同根因假设。

## 9. D5.2：kick 抢占门槛（受害者门控）

诊断：ungated kick 把每个符合条件的唤醒都变成抢占。N1 中受害者是
hog，kick 是收益；干净负载/混合负载中 kick churn 是纯开销。

设计：kick 仅在**受害者是 CPU hog（cpu_budget 超限）或已消耗过半
切片**时放行——hog 立即放行保 N1；半切片门槛即 EEVDF 抢占粒度。
实现：per-CPU `cpu_cur_victim` map 在 `running()` 快照受害者 hog
标志与半切片时间点，`enqueue()` 门控 `scx_bpf_kick_cpu`；被拦 kick
计入 `kick_gated`；`--no-kick-gate` 消融开关；echo 行新增 gate=
状态。`--hog-slice-us` / `--lag-carry-us` / `--no-kick` 用于参数消融。

### 9.1 gated 效果（2 轮定稿 + N2 全消融矩阵）

rwscaling（各配置两轮均值）：

| 档位 | V97 | lag0 | ungated lag50 | **gated lag50（定稿）** | gated vs V97 |
|---|---:|---:|---:|---:|---:|
| t32 | 9,990 | 9,550 | 9,005 | **9,272** | -7.2% |
| t64 | 12,436 | 11,499 | 11,054 | **11,829** | **-4.9%** |
| t128 | 12,561 | 11,352 | 11,128 | **11,760** | -6.4% |
| t256 | 12,446 | 11,268 | 10,735 | **11,470** | -7.8% |

gated 比 ungated 全面 +3~7%；p95 t256 54.36 vs ungated 63.32、
V97 30.27——t64 起尾部仍有差距（底座策略，见 9.2）。N1：noise p99 **0.21/0.30ms** 保持达标（kick_gated
拦掉约一半 kick，hog 受害者照常放行）。

N2 消融矩阵（FG rw64 + 8×BENCHMARK(SHA2)，120s mixed 窗口）：

| 变体 | solo p99 | mixed tps | mixed p99 | BG 完成数 | 判定 |
|---|---:|---:|---:|---:|---|
| V97（EEVDF） | 9.73 | 9,915 | 16.71 | 183 | 基准 |
| SN（smart mysqld+EEVDF） | 9.73 | 9,878 | 17.63 | 175 | smart mysqld 本身无赤字 |
| SG gated lag50（默认） | 15.83 | 9,136 | 47.47 | 176 | 当前最优 |
| SG ungated lag50 | 14.21 | 9,041 | 45.79 | 141 | gate 改善 tps/BG |
| SG gated lag0 | 20.37 | 6,499 | 84.47 | 353 | **lag carry 必需**（否则 FG 饿死） |
| SG gated nokick | 31.94 | 6,875 | 99.33 | 332 | **kick 必需**（同上） |
| SG gated cap20ms | 16.41 | 9,295 | 47.47 | 181 | cap 切片长度非尾部来源 |

### 9.2 归因结论

1. **N2 mixed 尾部三要素**：lag carry（84→47）与 kick（99→47）
   都是不可回退的保护机制；残余 47 vs EEVDF 16.7 的 3 倍差距来自
   **vtime 底座排序策略本身**——SN 与 V97 完全同档证明 mysqld 构建
   无赤字，差距全在调度器。这是 D4 时代即存在的干净负载 -5%
   同源问题（SG-lag0 rwscaling 即 -4~-9%）。
2. **分类器误判线索**：闭环 OLTP worker 事务内 ~20 个 query 微突发
   （CPU:间隔 > 1）使 cpu_budget 持续累积越过 20ms，mixed 中
   cpubound_cap ~500 万次（远超 8 个真 BG 所能解释）——FG worker
   被误判为 hog，在 kick gate 中被立即放行。nokick 消融证明这些
   kick 净收益为正，误判未直接致病，但说明 budget 累计式分类器
   不能区分"纯 CPU 循环"与"事务内微突发"。
3. **采纳决策：gated 为默认**——对 ungated 严格占优（rwscaling
   +5~7%，N1 持平达标，N2 持平，BG 完成数恢复 V97 量级）。

### 9.3 D7 候选方向（下轮设计）

- **真 EEVDF vlag 语义**：以 lag（应得服务-实得服务）决定资格与
  deadline，替代 cv-50ms clamp 近似——这是 mixed 47→17ms 与干净
  负载 -5% 的正解，需重写 enqueue/running/stopping 的 vtime 核心。
- **streak 式 hog 分类器**：按"连续烧满切片"（rt ≥ 0.9×slice 连续
  ≥2 次）判定 hog，替代 budget 累计——天然区分纯 CPU 循环与 OLTP
  微突发；该判定同时影响 cap 与 kick gate 放行。
- cpubound_cap 在内核 7.0 上 cap=20ms 仍触发 500 万次，说明
  fast_slice 返回值大于 20ms，SCX_SLICE_DFL 实际值与预期不符，
  D7 取证时先确认 DFL 值。
