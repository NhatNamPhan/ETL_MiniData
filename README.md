# FIFA21 ETL Pipeline

ETL (Extract - Transform - Load) pipeline xử lý dữ liệu cầu thủ FIFA21 và hiển thị Dashboard.

## 🏗️ Kiến Trúc

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Extract   │───▶│  Transform  │───▶│    Load     │
│   (CSV)     │    │  (Pandas)   │    │ (PostgreSQL)│
└─────────────┘    └─────────────┘    └─────────────┘
                                             │
                                             ▼
                                      ┌─────────────┐
                                      │  Dashboard  │
                                      │ (Streamlit) │
                                      └─────────────┘
```

## 📁 Cấu Trúc Project

```
ETL/
├── ETL.py              # Pipeline chính
├── dashboard.py        # Streamlit Dashboard
├── data/               # Thư mục chứa data
├── Dockerfile          # Docker image config
├── docker-compose.yml  # Multi-container setup
├── requirements.txt    # Python dependencies
└── .env                # Database credentials (không commit)
```

## 🚀 Cách Chạy

### Sử dụng Docker (Khuyến nghị)

```bash
# Build và start tất cả services
docker-compose up --build

# Hoặc chạy background
docker-compose up -d

# Dừng services
docker-compose down
```

### Chạy thủ công

```bash
# Tạo virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Linux/Mac

# Cài đặt dependencies
pip install -r requirements.txt

# Chạy ETL pipeline
python ETL.py

# Chạy Dashboard
streamlit run dashboard.py
```

## ⚙️ Cấu Hình

Tạo file `.env` với nội dung:

```env
DB_NAME=fifa_db
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
```

## 🔧 Services

| Service | Port | Mô tả |
|---------|------|-------|
| PostgreSQL | 5432 | Database |
| Dashboard | 8501 | Streamlit UI |

## 📊 Dashboard

Truy cập: **http://localhost:8501**

## 📝 License

MIT License
