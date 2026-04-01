# Inventory ETL System

> An automated ETL (Extract-Transform-Load) pipeline for inventory management that consolidates data from Google Sheets into BigQuery with MIS (Management Information System) and DSS (Decision Support System) capabilities.

## 🎯 Problem Statement & Solution

### Problems Solved

| Problem | Solution |
|---------|----------|
| **Input Delays & Data Errors** | Automated Python pipeline that validates, normalizes and loads data from Google Sheets to BigQuery every 10 minutes |
| **Fragmented Information** | Centralized Data Warehouse with Star Schema (Single Source of Truth) |
| **Lack of Decision Support** | Interactive BI Dashboards with Real-time MIS Reports and DSS Alerts |

---

## 🏗️ System Architecture

```
Google Sheets (Products, Prices, Partners, Orders)
        ↓
    ETL Pipeline (Python)
    • Extract from Google Sheets API
    • Transform & normalize columns (snake_case)
    • Validate data quality
        ↓
BigQuery Staging Layer (stg_products, stg_partners, stg_orders)
        ↓
BigQuery Data Warehouse (Star Schema)
    • Dimensions: dim_products, dim_partners
    • Fact Table: fact_orders (PURCHASE & SALE)
        ↓
BI Tools (Power BI / Looker Studio)
    • MIS Dashboards (Sales, Procurement, Partner 360)
    • DSS Alerts (Re-order Points, Cash Flow Optimization)
```

### Architecture Layers

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Data Entry** | Google Sheets | User-friendly data input UI |
| **ETL Pipeline** | Python + Google Sheets API | Extract, transform, validate |
| **Data Warehouse** | BigQuery | Centralized storage with Star Schema |
| **Analytics** | Power BI / Looker Studio | Interactive dashboards & reports |

---

## 📊 Data Model (Star Schema)

**Dimension Tables** (Static Data):
- `dim_products`: Product catalog (SKU, name, category, unit of measure)
- `dim_partners`: Partner management (Suppliers, Manufacturers, Customers)

**Fact Table** (Dynamic Data):
- `fact_orders`: Transaction records with order_type (PURCHASE/SALE), quantities, prices, taxes, and totals

---

## 📈 Dashboard & Reporting

### MIS Reports (Management Information System)

1. **Sales Performance Dashboard**
   - Filter: order_type = 'SALE'
   - Metrics: Revenue, order count, top products by margin

2. **Procurement Dashboard**
   - Filter: order_type = 'PURCHASE'
   - Metrics: Purchase costs, supplier spend distribution, delivery status

3. **Partner 360 Report**
   - Metrics: Receivables, sales volume, discounts by partner
   - Purpose: Customer/supplier relationship management

### DSS Alerts (Decision Support System)

1. **Re-order Alert System**
   - Highlights SKUs at Re-order Point (ROP) threshold
   - Auto-flags products needing urgent purchase orders

2. **Cash Flow Optimization**
   - Recommends inventory priority based on purchase price vs. retail price
   - Optimizes working capital rotation

---

## 🛠️ Tech Stack

- **Data Source**: Google Sheets API
- **Processing**: Python 3.9+
- **Libraries**: Pandas, PyArrow, Google Cloud client
- **Data Warehouse**: Google BigQuery
- **Orchestration**: Apache Airflow (10-minute intervals)
- **BI Tools**: Power BI / Looker Studio
- **Deployment**: Docker

---

## 📁 Project Structure

```
inventory_elt/
├── README.md                          # English documentation
├── README.vi.md                       # Vietnamese documentation
├── ETL_SYSTEM_SUMMARY.md             # Technical design document
├── requirements.txt                   # Python dependencies
├── config/
│   └── service_account.json          # Google Cloud credentials
└── src/
    ├── main.py                       # Pipeline orchestrator
    ├── extract.py                    # Extract phase
    ├── transform.py                  # Transform phase
    ├── validate.py                   # Validation rules
    ├── load.py                       # Load phase
    ├── test_extract.py               # Unit tests
    └── checkpoint.json               # State tracking (auto-generated)
```

---

## ⚙️ Quick Start

### Prerequisites
- Python 3.9+
- Google Cloud Project with Sheets API & BigQuery enabled
- Service Account credentials (JSON key file)

### Setup (5 steps)

```bash
# 1. Clone repository
git clone https://github.com/ginand/inventory_etl.git
cd inventory_etl

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure (create .env file in project root)
cat > .env << EOF
SPREADSHEET_ID=your_google_sheet_id
GCP_PROJECT_ID=your_gcp_project
BQ_DATASET=inventory_warehouse
GOOGLE_APPLICATION_CREDENTIALS=./config/service_account.json
EOF

# 5. Place service account JSON in config/ and run
python src/main.py
```

### Configuration

Required environment variables in `.env`:
```
SPREADSHEET_ID=<your-google-sheet-id>
GCP_PROJECT_ID=<your-gcp-project-id>
BQ_DATASET=inventory_warehouse
BQ_STAGING_DATASET=inventory_staging
GOOGLE_APPLICATION_CREDENTIALS=./config/service_account.json
```

### Running the Pipeline

```bash
# Manual run
python src/main.py

# With Airflow (scheduled every 10 minutes)
airflow dags list

# Docker deployment
docker build -t inventory-etl . && docker run -d inventory-etl
```

---

## � ETL Pipeline Phases

| Phase | Component | Function |
|-------|-----------|----------|
| **Extract** | `extract.py` | Read from Google Sheets API, apply checkpoint filtering |
| **Transform** | `transform.py` | Normalize columns (snake_case), convert data types |
| **Validate** | `validate.py` | Check required fields, referential integrity, format |
| **Load** | `load.py` | Write validated data to BigQuery stg_* tables |

**Validation Rules**: Required fields, numeric ranges, date formats, duplicate detection, foreign key references

**Error Handling**: Records with errors are logged separately; valid records continue through pipeline

---

## 📚 Data Dictionary

### Dimension Tables

**dim_products**
- `product_id`, `product_name`, `product_category`, `unit_of_measure`

**dim_partners**
- `partner_id`, `partner_name`, `partner_type`, `contact_person`, `payment_terms_days`, `lead_time_days`

### Fact Table

**fact_orders**
- `order_id`, `order_date`, `order_number`, `partner_id`, `product_id`
- `quantity`, `unit_price`, `tax_amount`, `total_line_amount`
- `order_type` (PURCHASE/SALE), `expected_date`

---

## 🔍 Checkpoint Management

The system uses `src/checkpoint.json` to enable incremental loading:

```json
{
  "products": "2024-01-15T10:30:00Z",
  "prices": "2024-01-15T10:30:00Z",
  "partners": "2024-01-15T10:30:00Z",
  "orders": "2024-01-15T10:30:00Z"
}
```

- Auto-generated on first run
- Updates after each successful load
- Enables resuming from last processed record

---

## 📚 Documentation

- **[ETL_SYSTEM_SUMMARY.md](./ETL_SYSTEM_SUMMARY.md)** - Technical architecture details
- **[README.vi.md](./README.vi.md)** - Vietnamese documentation
- **[Google Sheets API](https://developers.google.com/sheets/api)** - API reference
- **[BigQuery Docs](https://cloud.google.com/bigquery/docs)** - Data warehouse guide

---

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/improvement`
3. Commit changes: `git commit -m 'Add improvement'`
4. Push to branch: `git push origin feature/improvement`
5. Open Pull Request

---

## 📧 Support

- **Issues**: [GitHub Issues](https://github.com/ginand/inventory_etl/issues)
- **Documentation**: See [ETL_SYSTEM_SUMMARY.md](./ETL_SYSTEM_SUMMARY.md)

---

## 📄 License

MIT License - See LICENSE file for details

---

**Latest Update**: April 2026 | **Version**: 1.0.0
