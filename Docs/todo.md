# smart_sched v2 交接（2026-11-13）

当前分支 `smart_sched`（基线：上游 9.7 `008e09c28`）。v2 native-first
设计见 `Docs/smart-sched-design.md`，本轮完整测试结果与判定见
`Docs/smart-sched-v2-results.md`（先读这个）。

## 已完成（代码已提交，已推送 github）

- `6d89bcbc8` D1 bridge：等待边身份/引用计数/holder TLS 边栈
- `d240d88d4` D2 调度器：canonical vruntime + per-CPU vtime DSQ
- `07e43b7ea` D3 hog cap 默认 6ms（rw 实验依据）
- `47c083e06` D4 pin 全部 4 张 map（卡死取证路径）

## 测试结论（详见 results 文档 §1 总表）

- 达成：埋点零成本（SN 全场景持平 V97）；干净负载无回退
  （容量内开环 25k–250k 全档持平；N1/N2 干净基线持平）。
- 未达成：**负向韧性**——N1（stress-ng 干扰 p99 12–30 倍劣化）、
  N2（混部 p99 7 倍劣化）双双劣于原生 EEVDF；300k qps 确定性卡死。
- 语义层（SS）已证明在 N2 无效：boost 在打（semantic_hits 29 万/120s）
  但指标与 SG 相同。**别再调语义权重，先修底座。**

## 下一步（按优先级，接手从这里开始）

1. **D5（最高优先级）：底座对齐 EEVDF**——唤醒时 lag 携带
   （睡眠时长前移 eligible/虚拟截止期）+ 唤醒放置。这是 N1/N2 失败、
   rw -2.3%、300k 卡死的公共根因。改动集中在
   `support-files/scx/scx_db_sched.bpf.c`（task_account 里加 lag 字段，
   enqueue 路径保留 lag，running 时像 EEVDF 一样偿还）。
2. 300k 卡死定位：scx tracepoints / bpftrace 取证；D4 已把
   task_account_map/vtime_cpu pin 到 /sys/fs/bpf/scx_db_sched/，
   复现脚本在 /tmp/repro_stall.sh（已改用 pinned 路径）。
3. D5 落地后重跑全矩阵（run_v2.sh rwscaling/ratescan + run_v2_n1.sh
   + run_v2_n2.sh，V97 SN SG SS），验收标准不变：干净持平、
   负向不劣于 V97。lag 放置就位后把 hog cap 收紧回 2–3ms 重测 N2。
4. **D6：detach 看门狗**：loader 统计冻结/心跳消失时自动 detach
   回 EEVDF。没有它，"GUARDED"承诺不成立（300k 卡死需要手动干预）。

## 测试环境（重建方式）

- 脚本与经验：`/root/src/aliyun/mysql-sched-bench/`（provision.sh、
  build_smart.sh、run_v2*.sh、ENVIRONMENT.md 排障表）。
- 节点规格：16c `c8i.4xlarge`（DB）+ 8c `c8i.2xlarge`（loadgen），
  spot，`cn-wulanchabu`，镜像 Ubuntu 26.04 alibase，内核 7.0.0-38
  （EEVDF + sched_ext 可用）。**必须同规格同内核做对比**。
- 本地编译不了 v2 BPF（aarch64 + clang18 无 32-bit cmpxchg），
  loader/调度器验证一律在节点上 `cmake --build /opt/mysql-sched-smart/build
  --target scx_db_sched`。
- 纪律：测试完立即释放实例（先 rsync 结果并本地核验）；
  采购/排障细节见 ENVIRONMENT.md（spot 回收备份规范、公网 IP 白名单、
  ssh-keygen -R、后台任务超时上限 600s 要显式加长）。
