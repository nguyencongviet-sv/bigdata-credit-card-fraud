# CHECKPOINT 3: SPARK PROCESSING PIPELINE
**Đề tài:** Phát hiện gian lận thẻ tín dụng
**Nhóm:** Nhóm 9  
**Thành viên:** Đức, Thuần, Lộc  ,Việt ,Long

---

## 1. Kiến trúc Pipeline hiện tại (Modular PySpark Architecture)

Pipeline được thiết kế theo kiến trúc module hóa, điều phối tập trung qua tệp `src/pipeline.py` trên một `SparkSession` duy nhất và quản lý đường dẫn động qua `config/config.json`:

```text
[1. RAW CSV (3 files)] 
       │
       ▼ (quality_hduc.py)
[2. READ & PROFILING] ──► Explicit Schema (31 cột), kiểm tra Null/NaN & Class Distribution
       │
       ▼ (transform_thuan.py)
[3. CLEAN & TRANSFORM] ──► Filter lỗi, Dedup (Time, V1, V2, Amount), Derive (day_number, hour_of_day, risk_tier)
       │
       ▼ (join_quality_tloc.py)
[4. JOIN & QUALITY] ──► Broadcast Left Join với bảng Dimension risk_tier_dim (4 dòng), kiểm tra khóa & unmatched
       │
       ▼ (agg_vd.py)
[5. AGGREGATE & OUTPUT] ──► GroupBy (hour_of_day, risk_tier) -> Xuất Parquet, CSV, Physical Plan & quality_metrics.json
```

* **Input:** 3 tệp CSV tại `data/raw/` với **284,809** dòng giao dịch (~143.8 MB).
* **Processing:** Apache Spark DataFrame API (làm sạch, chuẩn hóa đặc trưng, nối bảng danh mục và tổng hợp đa chiều).
* **Output:** Bảng báo cáo phân tích tỷ lệ gian lận lưu định dạng Parquet (`output/summary_parquet/`), CSV (`output/summary_csv/`), kế hoạch thực thi (`output/physical_plan.txt`) và chỉ số chất lượng (`output/quality_metrics.json`).

---

## 2. Input, Schema & Transformations

### 2.1. Input Contract
* **Dataset:** Credit Card Fraud Detection (Kaggle).
* **Grain:** 1 dòng = 1 giao dịch thẻ tín dụng.
* **Quy mô:** 284,809 dòng / 3 tệp CSV (`Total nulls/NaNs: 0`).
* **Explicit Schema (31 cột):**
  * `Time`: `double` (số giây tính từ giao dịch đầu tiên).
  * `V1` .. `V28`: `double` (28 đặc trưng ẩn danh biến đổi từ PCA).
  * `Amount`: `double` (giá trị giao dịch tính bằng USD).
  * `Class`: `integer` (`0`: Giao dịch hợp lệ — 284,316 dòng / **99.8269%**; `1`: Gian lận — 493 dòng / **0.1731%**).

### 2.2. Transformations & Lý do nghiệp vụ (`transform_thuan.py`)
1. **Filter dữ liệu không hợp lệ:** Loại bỏ các bản ghi có giá trị Null/NaN, `Amount < 0` hoặc `Class` không thuộc `{0, 1}` để đảm bảo tính toàn vẹn toán học trước khi tính tổng/tỷ lệ.
2. **Khử trùng lặp (`dropDuplicates`):** Loại bỏ các giao dịch trùng lặp theo tổ hợp khóa `(Time, V1, V2, Amount)` nhằm tránh đếm lặp giao dịch làm sai lệch tỷ lệ gian lận.
3. **Trích xuất đặc trưng thời gian (`Derive`):** Chuyển đổi `Time` (giây) thành `day_number` (ngày thứ 1 hoặc 2) và `hour_of_day` (`0–23h`) để xác định khung giờ cao điểm xảy ra gian lận.
4. **Phân tầng hạn mức rủi ro (`risk_tier`):** Phân loại giao dịch dựa trên `Amount` thành 4 mức:
   * `MICRO` (`< $10`): 96,874 dòng (**34.14%**)
   * `LOW` (`$10 – < $100`): 129,578 dòng (**45.67%**)
   * `MEDIUM` (`$100 – < $500`): 47,817 dòng (**16.85%**)
   * `HIGH` (`>= $500`): 9,459 dòng (**3.33%**)

### 2.3. Quality Delta (Biến động số lượng bản ghi)
* **Input rows (Raw):** `284,809` dòng (100.00%)
* **Invalid / Null rows:** `0` dòng (0.00%)
* **Duplicate rows removed:** `1,081` dòng (0.3796%)
* **Valid rows (Accepted):** **`283,728`** dòng (**99.6204%**)

---

## 3. Join & Data Quality (`join_quality_tloc.py`)

### 3.1. Join Contract
* **Join Type:** `Left Outer Join` (sử dụng `F.broadcast(df_dim)` cho bảng danh mục nhỏ).
* **Join Key:** `risk_tier` (`MICRO`, `LOW`, `MEDIUM`, `HIGH`).
* **Unique Side:** Bảng Dimension `risk_tier_dim` (4 dòng danh mục với 4 khóa duy nhất, bổ sung thuộc tính `tier_description` và `alert_priority`).
* **Expected Relationship:** Many-to-One (`N:1`).

### 3.2. Kiểm soát chất lượng sau Join (Quality Gate)

| Chỉ số kiểm tra | Kết quả thực tế | Trạng thái / Chính sách xử lý |
| :--- | :---: | :--- |
| **Bảng Dimension (`risk_tier`)** | 4 dòng \| 4 duy nhất \| **0 khóa trùng** | Đảm bảo không gây nhân bản dòng (Row Explosion) |
| **Before Left (Fact Table)** | 283,728 dòng | Dữ liệu sạch từ Step 2 |
| **Before Right (Dimension Table)** | 4 dòng | Bảng danh mục phân tầng rủi ro |
| **After Join (Joined Table)** | **283,728 dòng** (Chênh lệch: `+0`) | Giữ nguyên 100% grain giao dịch ban đầu |
| **Null khóa Join & cột Dimension** | **0 dòng** | Toàn vẹn dữ liệu sau khi nối bảng |
| **Unmatched Records (`left_anti`)** | **0 dòng** (Tỷ lệ khớp: **100.0%**) | Policy: Giữ toàn bộ bản ghi Fact qua Left Join |

---

## 4. Aggregation & Output (`agg_vd.py`)

### 4.1. Bài toán & Cấu trúc tổng hợp
* **Câu hỏi nghiệp vụ:** *Khung giờ nào trong ngày và phân nhóm hạn mức giá trị giao dịch nào có số lượng và tỷ lệ gian lận cao nhất?*
* **Group Key:** `hour_of_day`, `risk_tier`, `tier_description`, `alert_priority`.
* **Output Grain:** 1 dòng = 1 tổ hợp `(Khung giờ × Phân tầng rủi ro)` → Tổng cộng **96 nhóm** (`24 giờ × 4 nhóm`).
* **Metrics tính toán:** `total_txns`, `fraud_txns`, `fraud_rate_pct`, `total_amount_usd`, `avg_amount_usd`, `max_amount_usd`.

### 4.2. Kết quả tổng hợp thực tế

#### A. Tổng hợp theo Phân tầng rủi ro (`risk_tier`)
| Mức ưu tiên | Phân nhóm (`risk_tier`) | Mô tả | Tổng giao dịch | Gian lận | Tỷ lệ gian lận (%) | Tổng giá trị (USD) |
| :---: | :--- | :--- | :---: | :---: | :---: | :---: |
| 1 | **MICRO** | Giao dịch siêu nhỏ (< $10) | 96,874 | **238** | 0.2457% | $358,668.26 |
| 2 | **LOW** | Giao dịch thông thường ($10 - $100) | 129,578 | 111 | 0.0857% | $5,013,026.75 |
| 3 | **MEDIUM** | Giao dịch trung bình ($100 - $500) | 47,817 | 91 | 0.1903% | $9,966,603.15 |
| 4 | **HIGH** | Giao dịch giá trị cao (>= $500) | 9,459 | 34 | **0.3594%** | $9,763,727.20 |

#### B. Top 5 khung giờ & phân nhóm có tỷ lệ gian lận cao nhất
| Giờ (`hour_of_day`) | Phân nhóm (`risk_tier`) | Tổng giao dịch | Giao dịch gian lận | Tỷ lệ gian lận (%) | Tổng giá trị (USD) |
| :---: | :--- | :---: | :---: | :---: | :---: |
| **2h** | **HIGH** | 72 | 2 | **2.7778%** | $91,840.05 |
| **4h** | **MICRO** | 585 | 16 | **2.7350%** | $2,648.05 |
| **2h** | **MICRO** | 1,271 | 31 | **2.4390%** | $4,575.50 |
| **5h** | **HIGH** | 59 | 1 | **1.6949%** | $52,474.54 |
| **3h** | **MICRO** | 954 | 15 | **1.5723%** | $4,667.88 |

> **Nhận xét nghiệp vụ:** Nhóm **HIGH (`>= $500`)** có tỷ lệ rủi ro cao nhất (**0.3594%**), đặc biệt vào lúc 02:00 sáng (2.78%). Trong khi đó, nhóm **MICRO (`< $10`)** chiếm tới **hơn 50% tổng số vụ gian lận (238/474 vụ)**, tập trung dày đặc từ 02:00 – 04:00 sáng (dấu hiệu điển hình của hành vi dò thẻ *card testing*).

### 4.3. Output Location & Chế độ ghi
* **Format:** Apache Parquet (lưu trữ chính) + CSV (preview kiểm tra).
* **Path:** `output/summary_parquet/` và `output/summary_csv/`.
* **Write Mode:** `overwrite` (đảm bảo tính Idempotent — chạy lại không sinh dữ liệu trùng lặp).

---

## 5. Baseline, Execution Plan & Next Action

### 5.1. Bảng đo thời gian Baseline (Buổi 8)
* **Môi trường thực thi:** `Local[*]` trên Windows | **Input size:** ~143.8 MB (3 files CSV) | **Shuffle Partitions mặc định:** 200.

| Giai đoạn xử lý | Module thực thi | Thời gian (s) | Tỷ trọng (%) |
| :--- | :--- | :---: | :---: |
| 1. Read Data & Quality | `quality_hduc.py` | 4.51s | 2.82% |
| 2. Transform & Cleaning | `transform_thuan.py` | 2.66s | 1.67% |
| 3. Join & Quality Check | `join_quality_tloc.py` | **86.68s** | **54.28%** |
| 4. Aggregate & Output | `agg_vd.py` | **61.47s** | **38.50%** |
| *Tổng thời gian thực thi 4 bước* | — | *155.32s* | *97.27%* |
| *Khởi tạo SparkSession & điều phối* | `pipeline.py` | *4.36s* | *2.73%* |
| **TỔNG THỜI GIAN TOÀN PIPELINE** | **End-to-End** | **159.68s** | **100.00%** |

### 5.2. Execution Plan & Current Blocker
* **Execution Plan:** Đã xuất đầy đủ kế hoạch thực thi vật lý tại `output/physical_plan.txt` và log chỉ số chất lượng tại `output/quality_metrics.json`.
* **Phân tích Plan:** Toán tử `BroadcastHashJoin` đã được kích hoạt cho bảng Dimension (4 dòng). Các nút `Exchange` (Shuffle 200 partitions) xuất hiện tại thao tác khử trùng lặp (`dropDuplicates`) và tổng hợp (`groupBy`).
* **Current Blocker:**
  1. Dữ liệu bị mất cân bằng nhãn nghiêm trọng (**0.1731% fraud**).
  2. Chưa sử dụng bộ nhớ đệm (`.cache()` / `.persist()`), khiến các lệnh Action ở Step 3 (`86.68s`) và Step 4 (`61.47s`) kích hoạt Spark đọc lại tệp CSV thô và tính toán lại toàn bộ DAG từ đầu, chiếm tới **92.8% tổng thời gian chạy**.
## 6. HƯỚNG DẪN THAO TÁC TERMINAL & MÔI TRƯỜNG (WINDOWS POWERSHELL)

### 6.1 Khởi tạo và kích hoạt môi trường ảo (`.venv`)
Đảm bảo đứng đúng thư mục chứa mã nguồn dự án (`DoAn_2`) trước khi thao tác:

```powershell
# 1. Di chuyển vào thư mục dự án khi đang ở môi trường của DoAn_2
cd ..\DoAn_3

# 2. Sử dụng chung môi trường ảo của DoAn_2
..\DoAn_2\.venv\Scripts\Activate.ps1
```
### 6.2 Chạy chương trình
```powershell
python src/pipeline.py
```
Chạy 3 lần tổng hợp
```powershell
$env:PYTHONUTF8=1

python src/backup.py
```
* **Git Commit:** `checkpoint-3: spark processing pipeline`