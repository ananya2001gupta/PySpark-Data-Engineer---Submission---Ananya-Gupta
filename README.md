# PySpark Data Engineer Assignment

## ✨ Overview

This project simulates a real-time data processing pipeline using PySpark on Databricks, integrated with AWS S3 and PostgreSQL. The pipeline ingests transaction data in chunks and detects customer behavior patterns as per business rules.

---

## ⚙️ Mechanisms

### ▶ Mechanism X: Ingest from GDrive to S3

- Reads 10,000 rows every second from `transactions.csv`
- Simulates a real-time feed
- Writes each chunk to S3 in separate folders

### ▶ Mechanism Y: Pattern Detection from S3 Stream

- Continuously reads ingested data from S3
- Detects 3 behavioral patterns:
  - **PatId1 - UPGRADE**: High-frequency, low-weight customers
  - **PatId2 - CHILD**: Low-value, high-frequency customers
  - **PatId3 - DEI-NEEDED**: Merchants with female representation gap
- Writes 50 detections per file to a separate S3 location

---

## 📚 Data Files

### `transactions.csv`

| Column          | Description                 |
| --------------- | --------------------------- |
| transactionId   | Unique transaction ID       |
| customerId      | Customer identifier         |
| customerName    | Customer's full name        |
| merchantId      | Bank or merchant identifier |
| amount          | Transaction amount in INR   |
| transactionType | Type of transaction         |
| gender          | Gender of the customer      |
| timestamp       | Timestamp of transaction    |
| fraud           | Ignored                     |

### `CustomerImportance.csv`

| Column          | Description                     |
| --------------- | ------------------------------- |
| customerId      | Customer identifier             |
| transactionType | Type of transaction             |
| weight          | Importance/weight score (float) |

---

## 🚀 How to Run

### Ὄb Setup

1. Upload the CSV files to `dbfs:/FileStore/` or mount to GDrive
2. Set up AWS S3 bucket with write access
3. Set up PostgreSQL (hosted locally or on RDS)

### 📂 Notebooks

| Notebook                   | Purpose                                    |
| -------------------------- | ------------------------------------------ |
| `mechanism_x_ingest.ipynb` | Simulates data ingestion from GDrive to S3 |
| `mechanism_y_detect.ipynb` | Detects patterns from streamed data        |

### 🔐 PostgreSQL

Use the script in `sql/init_postgres_tables.sql` to create:

- `merchant_txn_count`
- `customer_txn_stats`
- `detected_patterns`

---

## 📊 Architecture Diagram

```
+------------------+        +-------------------+        +-------------------------+
|  GDrive CSV Data |  -->   |  Mechanism X      |  -->   |   S3 Batch Layer        |
+------------------+        +-------------------+        +-------------------------+
                                                          ||
                                                  Read every second
                                                          \/
                                              +---------------------------+
                                              |  Mechanism Y (Streaming)  |
                                              +---------------------------+
                                                |        |          |
                                        Pattern 1    Pattern 2   Pattern 3
                                                \        |         /
                                                 \       |        /
                                                Output Detections
                                                to S3 (50 per file)
```

---

## 📅 Output Example

```json
{
  "YStartTime": "2025-07-04T12:00:01",
  "detectionTime": "2025-07-04T12:00:03",
  "patternId": "PatId2",
  "ActionType": "CHILD",
  "customerName": "Alice Sharma",
  "merchantId": "B123"
}
```

---

## 🔍 Assumptions

- Each transaction is atomic and real-time ordering isn’t critical
- PostgreSQL is used only for state management (not detection)
- One detection per customer per merchant per pattern

---

## 📄 Submission Checklist

-

---

## 👨‍💻 Author

Ananya Gupta
PySpark Data Engineer Candidate


