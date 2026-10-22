ư# Đồ án 2: Phát hiện giao dịch bất thường thẻ tín dụng (Credit Card Fraud Detection)
Checkpoint 2: Environment -> Ingestion -> Raw Storage

## 1. Cấu trúc thư mục
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