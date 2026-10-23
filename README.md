# Đồ án 2: Phát hiện giao dịch bất thường thẻ tín dụng (Credit Card Fraud Detection)
Checkpoint 2: Environment -> Ingestion -> Raw Storage

## 1. Cấu trúc thư mục

```text
D:\bigdata_credit\
├── .gitignore                      # Cấu hình bỏ qua .venv và file CSV gốc lớn
├── README.md                       # Hướng dẫn thực thi dự án
├── requirements.txt                # Khai báo phụ thuộc (pyspark==4.2.0, v.v.)
├── data/
│   ├── source_input/
│   │   └── creditcard.csv          # File nguồn gốc từ Kaggle (284,807 records)
│   └── raw/
│       └── credit_card_transactions/
│           └── ingest_date=YYYY-MM-DD/
│               └── run_<TIMESTAMP>/
│                   └── raw_transactions.csv  # Raw storage preserved
├── logs/
│   ├── env_check.txt               # Bằng chứng Smoke Test môi trường
│   └── ingest_credit_card.log      # Log thực thi Ingestion Pipeline
├── metadata/
│   └── manifests/
│       └── manifest_run_<TIMESTAMP>.json # File lưu vết SHA-256, count, status
└── src/
    ├── env_check.py                # Script kiểm tra Smoke Test (Tầng 5)
    └── ingest_credit_card.py       # Script nạp dữ liệu & kiểm tra Quality Gate
```

---

- `src/`: Chứa mã nguồn kiểm tra môi trường và nạp dữ liệu.
- `data/raw/`: Chứa dữ liệu gốc theo phân vùng `ingest_date` và `run_id`.
- `logs/`: Lưu trữ file log quá trình chạy.
- `metadata/manifests/`: Lưu vết manifest (JSON) phục vụ kiểm tra provenance.

## 2. Yêu cầu Môi trường
- Python 3.10+
- PySpark 3.5+ / 4.x
- Java JDK 11 hoặc cao hơn

## 3.Nguồn dữ liệu (Data Source & Provenance)
* **Bộ dữ liệu gốc:** Credit Card Fraud Detection Dataset (Kaggle).
* **Nguồn dữ liệu gốc (URL):** [https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud)
* **Vị trí tệp nguồn local:** `data/source_input/creditcard.csv`
* **Mô tả:** Tập dữ liệu bao gồm các giao dịch thẻ tín dụng của chủ thẻ Châu Âu thực hiện trong tháng 09/2013. Tập dữ liệu gồm **284,807 bản ghi**, trong đó có **492 giao dịch gian lận** (`Class = 1`, chiếm tỉ lệ cực nhỏ ~0.172% -> bài toán Class Imbalance).

---
## 4. Hướng dẫn Thực thi từng bước (Execution Guide)

### Bước 1: Kích hoạt môi trường ảo Python (`.venv`)
Mở PowerShell tại thư mục gốc `D:\bigdata_credit` và chạy:

```powershell
.\.venv\Scripts\Activate.ps1
```
*(Đảm bảo hiển thị tiền tố `(.venv)` ở đầu con trỏ Terminal).*

---

### Bước 2: Chạy Smoke Test kiểm tra Môi trường (File 1)

Thực thi script `src/env_check.py` để kiểm tra toàn bộ 5 tầng môi trường (OS, Java, Python, PySpark, Spark Job) và xuất file bằng chứng log:

```powershell
python src/env_check.py | Out-File -FilePath logs/env_check.txt -Encoding utf8
```

**Kiểm tra kết quả:**
```powershell
Get-Content logs/env_check.txt
```
*Kỳ vọng Output:*
```text
Starting Spark Environment Smoke Test...

========================================
ENVIRONMENT SMOKE TEST: PASS
Python Version: 3.12.5
Spark Version : 4.2.0
Test Execution: Processed 2 records successfully
========================================
```

---

### Bước 3: Chạy Pipeline Nạp Dữ liệu & Lưu trữ Tầng Raw (File 2)

Thực thi script `src/ingest_credit_card.py` để tiến hành nạp dữ liệu thật từ Kaggle, đi qua Quality Gate, phân vùng lưu trữ tầng Raw và sinh Manifest tracking:

```powershell
python src/ingest_credit_card.py
```

**Sản phẩm đầu ra (Artifacts) được sinh ra:**
1. **Raw Storage:** `data/raw/credit_card_transactions/ingest_date=YYYY-MM-DD/run_<TIMESTAMP>/raw_transactions.csv`
2. **Manifest File:** `metadata/manifests/manifest_run_<TIMESTAMP>.json` (Chứa thông số 284,807 bản ghi, mã băm SHA-256, dung lượng file, và status `"SUCCESS"`).
3. **Execution Log:** `logs/ingest_credit_card.log`

---
