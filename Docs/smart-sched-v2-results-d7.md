# smart_sched v2 — D7 轮结果（2026-11-17）

Campaign: `mysql-sched-d7-20261117`（16c `ecs.c9i.4xlarge` DB + 8c `ecs.c9i.2xlarge`
LG，双 spot，Ubuntu 26.04 alibase，内核 7.0.0-38-generic，V97/SN/SG 共用同一
镜像内核与实例）。上轮（D5/D6）结论见 `smart-sched-v2-results-d5d6.md` §9.3。

本轮目标（上轮遗留）：修干净负载 -5~-8% 与 N2 mixed 尾部 47ms 的
"vtime 底座排序策略" 根因。**8 个代码迭代、6 个 N 场景修复轮之后，本轮
把它归因到底座参照系（max-floor vs EEVDF 的 avg_vruntime）与 hog 判定的
边界上，而不是某个可调参数。**

## 0. 结论速览

| 场景 | V97 | SG 定稿（D7.6+bs1000） | 判定 |
|---|---:|---:|---|
| rw t32 | 10,112 | 10,086 (-0.3%) | ✅ 持平 |
| rw t64–t256 | 12,557/12,649/12,475 | 11,895/11,912/11,517 | ❌ -5.3~-7.7%（结构性，见 §5） |
| ratescan 250k/300k | 250,217/299,935 | 249,946/300,048 | ✅ 持平，无卡死 |
| N1 noise p99 @10k/50k | 0.21/0.46ms | **0.18/0.18ms** | ✅ **优于原生**（上轮 0.21/0.30） |
| N2 solo tps / p99 | 12,390 / 9.22 | 11,803 / 12.75 | ❌ 尾部未达标 |
| N2 mixed tps / p99 | 10,013 / 13.95 | 10,204 / 34.33 | ❌ 尾部未达标（上轮 47.47，改善 28%） |
| N2 BG 完成数 | 182 | 123 | ✅ 不饿死 |
| N2 history list | 483→2,368→350 | 100k→403k→74k | ⚠️ **饱和窗口 purge 饿死，D8 首要问题** |

## 1. rwscaling（oltp_read_write 闭环，2 轮均值）

| 档位 | V97 tps | D7.3@默认切片 | D7.3+bs1000 | D7.6+bs1000 | V97 p95 | D7.3+bs1000 p95 |
|---|---:|---:|---:|---:|---:|---:|
| t32 | 10,112 | 10,251 (+1.4%) | 10,190 (+0.8%) | 10,086 (-0.3%) | 3.99 | 3.69 |
| t64 | 12,557 | 11,919 (-5.1%) | 11,461 (-8.7%) | 11,895 (-5.3%) | 7.17 | 8.58 |
| t128 | 12,649 | 11,955 (-5.5%) | 11,445 (-9.5%) | 11,912 (-5.8%) | 14.60 | 21.70 |
| t256 | 12,475 | 11,602 (-7.0%) | 11,169 (-10.5%) | 11,517 (-7.7%) | 31.09 | 42.23 |

对 D5.2（gated lag50，rw -4.9~-7.8%、t256 p95 54.4ms）：D7.3@默认切片在
**所有档位 tps 严格更优**，且 p95 全面更好（t64 8.66 vs 10.28，t256 47.5 vs 54.4）。
bs1000 把 t128/t256 的 p95 再压到 21.7/42.2，代价是深队列 ~3.5% tps
（韧性优先，取 bs1000 为缺省，`--base-slice-us 0` 可回退）。

消融记录（1–2 轮）：
- D7.0（frozen-vlag，CV 无推进）：+1.9 / -7.2 / -9.2 / -8.4%，p95 3.42/10.27/28.42/50.57
- D7.1（cv 随服务推进）：+2.3 / -7.0 / -8.8 / -8.3% —— 推进 cv 本身对 tps 无感
- D7.1+bs1000：t64-256 11,648/11,500/11,383，p95 3.55/10.65/20.55/45.39
- t32 的 +1.4~2.3%（D5.2 时 -7.2%）来自 streak 分类器消除 worker 误判，
  与切片/lag 语义无关。

## 2. ratescan（开环 point_select）

250k/300k 两档 V97 与 SG-bs1000 全档持平（p95 0.15–0.17ms），300k 无
"queue full"、无卡死。上轮 300k 卡死未复现。✅

## 3. N1（16×stress-ng 干扰 + 开环 point_select）

| 变体 | noise p99 @10k/50k | 判定 |
|---|---:|---|
| V97（原生 EEVDF） | 0.21 / 0.46 | 基准 |
| D7.1+frozen-vlag（纯 EEVDF 语义） | 1.04 / 2.03 | ❌ 长睡眠 waker 无信用，等满切片 |
| D7.2（睡眠信用 + 闪烁 streak） | 1.04 / 1.67 | ❌ hog 被 kick 打断即清零 → gate 闪烁 |
| D7.3（last_full_run gate） | **0.19 / 0.18** | ✅ |
| D7.3@默认切片 | 0.40 / 0.54 | ⚠️ 切片节奏相关 |
| **D7.6（定稿）** | **0.18 / 0.18** | ✅ 优于原生 |

修复链条的三个认知（详细机理见各 commit message）：
1. **纯 EEVDF 冻结 vlag 是错的**：vlag 冻结于入睡时刻（≈0），睡眠时长被
   赦免 → 长睡眠交互线程零信用、deadline 输给 hog → 等满一个切片。
   有界睡眠信用（D5.2 机制）才是 N1 的正确保护。
2. **streak 不能因"被抢占"清零**（闪烁），也不能因"阻塞"保留（purge 误判）——
   正确语义：烧满切片累加、被抢占保留、**阻塞清零**。
3. SCX_SLICE_DFL 取证：内核 7.0.0-38 实际值 **20ms**（smoke banner 打印，
   loader 从 BTF 运行时解析）。D5/D6 时代 "cap=20ms 仍触发 500 万次" 完全是
   budget 误判，与 DFL 无关。

## 4. N2（FG rw64 + 8×BENCHMARK(SHA2)，120s 窗口）

| 变体 | solo tps | solo p99 | mixed tps | mixed p99 | BG 完成数 | history（solo→mixed） |
|---|---:|---:|---:|---:|---:|---|
| V97 | 12,390 | 9.22 | 10,013 | 13.95 | 182 | 483→2,368→350 |
| D7.3（last_full_run） | 11,356 | 12.08 | 10,083 | 33.72 | 110 | 125.6k→490.2k→176.7k |
| D7.5（阻塞清零 streak） | 11,785 | 12.52 | 10,274 | 32.53 | 115 | 118.7k→381.3k→116.4k |
| D7.6（全量 floor，定稿） | 11,803 | 12.75 | 10,204 | 34.33 | 123 | 100.1k→403.2k→74.0k |

**本轮最重要的发现：purge 饿死**。饱和窗口（solo）内 history list 线性
膨胀至 V97 的 200 倍，机理（D7.6 消融链收敛）：

1. 每个 waker 携带 cv−50ms 睡眠信用入队；连续 runnable 的后台线程
   （InnoDB purge worker：批处理 memcpy 为主、1ms 切片下 ≥2 个连续烧满
   不阻塞）被 streak 判为 hog。
2. **hog 被豁免在 vtime floor 之外**（豁免是 N1 必需的：正是 hog 推进的
   base 让 waker 在排序与 kick deadline 检查中胜出）。
3. 于是 purge 的 base 随运行推进到 cv+ε，排在所有带信用 waker 之后，
   独占 CPU 的 FG 越饱和饿得越狠（"信用底层阶级"）。
4. D5.2 时代 purge 健康（history ~2k），是因为其 clamp 在**每次 re-enqueue**
   生效且 20ms 切片下 purge 的阻塞节奏不会积累 budget；bs1000 的 1ms
   切片把同一节奏变成"连续烧满"。

mixed 窗口 p99 34.33 相对 D5.2 的 47.47 改善 28%（kick 刻度修复 + streak
分类器），但距 V97 13.95 仍有 2.5×，其中相当一部分是 history 膨胀带来的
MVCC 回溯成本（不纯粹是排队延迟）。BG 完成数 123（V97 182）不饿死。

## 5. 结构性 -5~-10% 的归因（BPF 成本取证）

`kernel.bpf_stats_enabled=1` 实测（rw64 饱和窗口）：

| 回调 | run_time/次 |
|---|---:|
| select_cpu | 280ns |
| enqueue | 376ns |
| dispatch | 334ns |
| running | 198ns |
| stopping | 155ns |

五个回调合计 ≈ 总 CPU 的 **2%**（14.3M 次调用/窗口）。排除 BPF 程序本身后，
剩余缺口在 sched_ext 框架层：per-CPU DSQ 的 rb-tree 插入/锁、6.4M 次
dispatch steal 的跨 CPU DSQ 锁竞争、select_cpu_dfl 的放置开销。这是
sched_ext 抽象（DSQ/回调）相对原生内联 runqueue 的固有税，策略层调不掉。
（取证副产品：`perf stat -p` 的软件计数器在 400 万次上下文切换/30s 的
负载下自损 30% 吞吐，不可用——已记入 ENVIRONMENT.md。）

## 6. 机制定稿（D7.6，commits 7c2ea8b61..83139cfdc）

- **wakeup 信用**：睡眠时长计入 entitlement，`base = cv − min(sleep, 50ms)`
  （`--frozen-vlag` 保留纯 EEVDF 变体做消融）；
- **全量 floor**：每次 enqueue（含切片到期 re-enqueue）把非 hog 任务
  拉回 `cv − 50ms`，突发型后台线程与 waker 平权；
- **cv 随已执行服务推进**（stopping 时推进到底座），floor 不冻结、
  深队列排序退化为 FIFO 而非插入序；
- **streak hog 判定**：连续烧满 ≥90% granted slice ≥2 次；被抢占保留、
  阻塞清零；hog 才被豁免 floor、吃 6ms cap、立即放行 kick；
- **kick**：deadline 比较与 victim 同刻度（fast_slice 修复）+ 1ms
  wakeup 粒度门；
- **bs1000 缺省**：fast_slice 档位基切片 1ms（`--base-slice-us 0` 回退内核
  SCX_SLICE_DFL=20ms），对齐原生 0.75ms 节奏，换尾部与 N1。

## 7. D8 方向（按优先级）

1. **avg_vruntime 参照系**（purge 饿死与信用底层阶级的正解）：把底座参照
   从 per-CPU max-floor 换成负载加权平均（原生 EEVDF 的做法）。waker 放置
   在 avg−lag，hog 的 vruntime 天然高于 avg，waker 不需要任何"睡眠信用"就
   能在排序上压过 hog——50ms 信用、hog 豁免、底层阶级三个补丁同时消失。
   BPF 侧需要维护 per-CPU 的 Σw·vruntime/w_total 增量结构（enqueue/dequeue
   钩子可做），是底座级重写。
2. **purge 兜底**（avg 落地前的过渡）：mysqld 侧把维护类线程（purge、
   master、IO）通过 bridge 打轻量标记，调度器对"DB 维护线程"保底公平
   （不饿死），语义层方案（SS）复用现有 bridge 通道。
3. **结构性 tps 税**：dispatch steal 的锁竞争（6.4M steals）可试验
   batch dispatch / locality-first 顺序；与上游 scx 社区对齐。
4. tpc-c 场景与 N3/N4（网络延迟抖动、连接风暴）本轮未测，下轮补。

## 8. 复现

- 节点：见 `../aliyun/mysql-sched-bench/campaign-d7-20261117.env` 与
  `d7.env`；run 脚本 `run_v2.sh`（rwscaling/ratescan）、`run_v2_n1.sh`、
  `run_v2_n2.sh`，消融经 `SG_EXTRA_ARGS` 透传。
- 本地结果：`../aliyun/mysql-sched-bench/results/v2-20261117/`（rwscaling
  含 SG-d7.0/d7.1/d7.1-bs1000/d7.3-final/d7.3-bsdef-t64/d7.3/d7.6）、
  `v2-n1-20261117/`（V97/SG-frozenvlag/SG-d72/SG-d73-bs1000/SG@默认切片/SG-d7.6）、
  `v2-n2-20261117/`（V97/SG-d73/SG-d75/SG-d76）。
- 本轮 commits（smart_sched）：7c2ea8b61, e3e7a6a69, aaa683c64, 7e0cd0146,
  cd2f39f4e, 780af5af0, a3305488d, ed07b49b7, 83139cfdc。
