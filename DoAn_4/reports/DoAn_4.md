# ĐỒ ÁN 4: BENCHMARK & TỐI ƯU PIPELINE

**Đề tài:** Phát hiện gian lận thẻ tín dụng
**Nhóm:** Nhóm 9  
**Thành viên:** Đức, Thuần, Lộc  ,Việt ,Long

| Chỉ số tổng quan | Giá trị đạt được | Ghi chú |
| :--- | :--- | :--- |
| **MEDIAN RUNTIME** | **17.73 s** | Giảm từ 184.73 s |
| **SPEEDUP** | **90.4 %** | Nhanh gấp 10.4× toàn pipeline |
| **CORRECTNESS** | **PASS** | 0 sai lệch dữ liệu & chỉ số nghiệp vụ |

---

## 01 • Baseline

> *Bối cảnh đầy đủ để tái lập kết quả benchmark chuẩn.*

### Workload
* **Dataset:** Credit Card Fraud (Kaggle) — 284,807 dòng, ~143.8 MB (3 file CSV)
* **Input path:** `DoAn_2/data/raw/credit_card_transactions/*/*/*.csv`
* **Output target:** Parquet + CSV (96 nhóm), `physical_plan.txt`, `quality_metrics.json`
* **Pipeline:** Read & Profile $\rightarrow$ Clean/Dedup ($-1,081$ dòng) $\rightarrow$ Broadcast Left Join `risk_tier_dim` $\rightarrow$ `GroupBy(hour_of_day, risk_tier)`
* **Rows sau clean:** 283,726 (99.62%) / 283,728 bản ghi thực thi

### Environment
* **Spark version:** 4.2.0 / 3.5+ (PySpark), Java 17 Temurin
* **Master / executors:** `local[*]` — 1 JVM, không có executor riêng
* **Cores / memory:** 4 vCPU / 15 GiB RAM (GitHub Actions `ubuntu-latest` / Local Windows); driver memory mặc định
* **Important configs:** `spark.sql.adaptive.enabled=true` • `spark.sql.shuffle.partitions=16` • không cache/persist • write mode `overwrite`
* **Shuffle Parts:** 16 partitions (20 scan)

### Baseline Results (3 Runs)
* **Run 1:** `181.71 s`
* **Run 2:** `184.73 s` *(Median)*
* **Run 3:** `185.00 s`
* **Step 3 & 4 Median:** `100.30 s` + `70.15 s` = `170.45 s` (chiếm 92.3% tổng thời gian)
* **Rows Out:** 283,728 sạch $\rightarrow$ 96 nhóm
* **Business metrics:** 474 fraud (`$15.72M`)

---

## 02 • Bottleneck Evidence

> *Tính toán lặp lại (Repeated Computation) qua 10 Actions ở Step 3–4.*

### Observation
* Step 3 (`100.30 s`) và Step 4 (`70.15 s`) chiếm **92.3%** tổng runtime mặc dù phép Join đã sử dụng `BroadcastHashJoin`.

### Bottleneck Claim
* Step 3–4 chậm vì thiếu `.cache()` sau bước `dropDuplicates`, khiến **10 Actions** (5 Actions ở Step 3 + 5 Actions ở Step 4) phải đọc lại file CSV từ ổ cứng và thực hiện lại bước shuffle `dropDuplicates` từ đầu.

### Evidence • Physical Plan (Baseline)
* **Path:** `DoAn_3\output\physical_plan.txt`
* **Operators liên quan:** `(1) Scan csv` $\rightarrow$ `(4) Exchange(16)` lặp lại ở mọi Action do thiếu `InMemoryTableScan`.

```text
== Physical Plan ==
AdaptiveSparkPlan (16)
+- Sort (15)
   +- Exchange (14)
      +- HashAggregate (13)
         +- Exchange (12)
            +- HashAggregate (11)
               +- Project (10)
                  +- BroadcastHashJoin LeftOuter BuildRight (9)
                     :- Project (6)
                     :  +- HashAggregate (5)
                     :     +- Exchange (4)
                     :        +- HashAggregate (3)
                     :           +- Filter (2)
                     :              +- Scan csv  (1)
                     +- BroadcastExchange (8)
                        +- Scan ExistingRDD (7)
```

---

## 03 • Hypothesis & Change

> *Một experiment = một thay đổi chính.*

### Hypothesis
* **Nếu** Step 3–4 chậm vì chạy lại CSV scan và `dropDuplicates` qua 10 Actions, **thì khi** thêm `df_clean.cache()` ở cuối Step 2, **chúng tôi kỳ vọng** Physical Plan sẽ sử dụng `InMemoryTableScan` và runtime giảm $>60\%$, đồng thời giữ nguyên 283,728 dòng sạch.

### Change (Code Diff)
```python
# Before:
df_clean = df_dedup.withColumn(...)

# After:
df_clean = df_dedup.withColumn(...).cache()

# Cleanup (cuối pipeline):
df_clean.unpersist()
```

### Expected Signal & Control
* **Signal:** Xuất hiện `InMemoryTableScan` trong Physical Plan; Shuffle Read ở Step 3–4 giảm về 0.
* **Control:** Giữ cố định dữ liệu đầu vào (3 file CSV), chế độ `local[*]`, và cấu hình `spark.sql.shuffle.partitions=16`.

---

## 04 • Before / After

> *Đo nhiều lần và báo cáo giá trị trung vị (Median 3 Runs).*

| Metric | Baseline (Đồ án 3) | After (Median 3 Runs) | $\Delta$ (Thay đổi) |
| :--- | :--- | :--- | :--- |
| **Runtime median (Tổng pipeline)** | `184.73 s` *(181.71 / 184.73 / 185.00)* | **`17.73 s`** *(17.13 / 17.73 / 18.17)* | **`-90.4%` (`10.4×`)** |
| **Step 1 (Read) / Step 2 (Clean)** | `5.26 s` / `2.99 s` | `4.82 s` / `4.81 s` | `+1.38 s` *(Materialize)* |
| **Step 3 (Join) / Step 4 (Aggregate)** | `100.30 s` / `70.15 s` | **`1.07 s` / `2.58 s`** | **`-97.9%` (`-166.8 s`)** |
| **Physical Plan & Cache State** | `Scan csv` $\rightarrow$ `Exchange(16)` | **`InMemoryTableScan` (`127.7 MiB`)** | Loại bỏ re-compute |
| **Output rows & Business metric** | `283,728` / `474 fraud` (`$15.72M`) | `283,728` / `474 fraud` (`$15.72M`) | `0` *(Khớp 100%)* |

### Đối chiếu Execution Plan
* **Plan trước (Baseline — `DoAn_3\output\physical_plan.txt`):**
  ```text
  BroadcastHashJoin (9) ← HashAggregate (5) ← Exchange(16) (4) ← Filter (2) ← Scan csv (1)
  ```
* **Plan sau (After - Đã Cache — `DoAn_4\output\physical_plan.txt`):**
  ```text
  BroadcastHashJoin (20) ← InMemoryTableScan (1) ← InMemoryRelation (2) [127.7 MiB, 2.84E+5 rows]
  ```

---

## 05–06 • Correctness & Decision

> *Kết thúc bằng quyết định kỹ thuật dựa trên bằng chứng thực nghiệm.*

### Correctness (PASS)
* **Row counts:** `283,728` dòng sạch / sau Join (khớp tuyệt đối).
* **Key metrics:** `96` nhóm / `474` giao dịch gian lận (`$15.72M`).
* **Schema:** Đủ 10 cột chuẩn cho cả định dạng Parquet & CSV.
* **Checksum / Unmatched:** `0` bản ghi lệch.

### Decision: `KEEP`
* Giữ lại thay đổi `.cache()` vì giúp giảm **90.4%** median runtime (`184.73 s` $\rightarrow$ `17.73 s`) với **0 regression**.

### Limitations
* Đánh đổi tiêu tốn **127.7 MiB RAM** để lưu trữ `df_clean`. Khi kích thước dữ liệu tăng $100\times$ (~14 GB), cần chuyển sang `StorageLevel.MEMORY_AND_DISK` hoặc gộp các bước kiểm tra `count()` thành một lần `.agg()` duy nhất.

### Next Action (Buổi 11)
* Cân nhắc chuyển đổi định dạng lưu trữ nguồn từ CSV sang **Parquet** (columnar storage), tổ chức kiến trúc **Data Lake** (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) / NoSQL và đánh giá tác động đến hiệu năng toàn pipeline.

## 3 Hướng dẫn thực thi

Đảm bảo đứng đúng thư mục chứa mã nguồn dự án (`DoAn_2`) trước khi thao tác:

```powershell
# 1. Di chuyển vào thư mục dự án khi đang ở môi trường của DoAn_2
cd ..\DoAn_4

# 2. Sử dụng chung môi trường ảo của DoAn_2
..\DoAn_2\.venv\Scripts\Activate.ps1
```
### Chạy chương trình
```powershell
$env:PYTHONUTF8=1

python src/backup.py
```