# STEDI Human Balance Analytics

This project focuses on building a data lakehouse solution using **AWS Glue**, **S3**, **Python**, and **Spark** to process and curate sensor data for training a machine learning model. The project emulates the work of a data engineer managing data pipelines for the STEDI Step Trainer, which combines motion sensors and a mobile app to collect and process balance training data.

## Table of Contents

- [Project Overview](#project-overview)
- [Project Workflow](#project-workflow)
- [Setup Instructions](#setup-instructions)
- [Data Flow](#data-flow)
- [Key Components](#key-components)
- [Validation and Outputs](#validation-and-outputs)
- [File Structure](#file-structure)
- [Screenshots and Deliverables](#screenshots-and-deliverables)

---

## Project Overview

The STEDI Step Trainer system:
- Collects motion sensor data during balance exercises.
- Uses a mobile app to capture accelerometer readings.
- Integrates the data for training a machine learning model to detect steps accurately.

### Objectives
- Extract data from multiple landing zones.
- Sanitize and curate the data into trusted and curated zones in S3.
- Build Glue jobs to transform, clean, and prepare data.
- Create an aggregated dataset for machine learning.

---

## Project Workflow

1. **Data Sources**:
   - Customer data (`customer_landing`).
   - Accelerometer data (`accelerometer_landing`).
   - Step Trainer data (`step_trainer_landing`).

2. **ETL Pipeline**:
   - Extract and load data into AWS S3 landing zones.
   - Create Glue tables to query semi-structured data.
   - Transform and sanitize data into trusted and curated zones.

3. **Data Curation**:
   - Filter customer and accelerometer data based on sharing consent.
   - Resolve data quality issues (e.g., duplicate serial numbers).
   - Join and aggregate Step Trainer and accelerometer readings.

4. **Outputs**:
   - Glue tables for trusted and curated datasets.
   - Machine learning-ready data.

---

## Setup Instructions

### Prerequisites
- AWS account with S3 and Glue services enabled.
- Python 3.x installed locally for testing scripts.
- Access to AWS Glue Studio and Athena.

### Steps
1. **Setup Landing Zones**:
   - Create S3 directories for `customer_landing`, `accelerometer_landing`, and `step_trainer_landing`.
   - Upload the respective datasets to these directories.

2. **Create Glue Tables**:
   - Write SQL scripts for each landing zone and create Glue tables.
   - Verify tables by querying them in Athena.

3. **Run Glue Jobs**:
   - Develop and deploy Glue jobs for data transformation:
     - Customer trusted data.
     - Accelerometer trusted data.
     - Customers curated data.
   - Validate job outputs at each stage.

4. **Aggregate Data**:
   - Write Glue jobs to join Step Trainer and accelerometer data.
   - Produce the final dataset for machine learning.

---

## Data Flow

### Landing Zones
- **Customer**: Raw customer data from the website.
- **Accelerometer**: Sensor data from mobile apps.
- **Step Trainer**: IoT data from Step Trainer devices.

### Trusted Zone
- Cleaned data with only records from customers who consented to share their data.

### Curated Zone
- Finalized datasets ready for machine learning training:
  - Joined Step Trainer and accelerometer readings.
  - Includes only records from consented customers.

---

## Key Components

1. **AWS Glue**:
   - Jobs to process, clean, and aggregate data.
   - SQL queries for efficient transformations.

2. **S3**:
   - Storage for landing, trusted, and curated zones.

3. **Athena**:
   - Query Glue tables to validate and inspect data.

4. **Python and Spark**:
   - Code for custom ETL logic in Glue scripts.

---

## Validation and Outputs

| Table                 | Zone       | Expected Rows |
|-----------------------|------------|---------------|
| `customer_landing`    | Landing    | 956           |
| `accelerometer_landing` | Landing  | 81,273        |
| `step_trainer_landing` | Landing   | 28,680        |
| `customer_trusted`    | Trusted    | 482           |
| `accelerometer_trusted` | Trusted | 40,981        |
| `step_trainer_trusted` | Trusted  | 14,460        |
| `customers_curated`   | Curated    | 482           |
| `machine_learning_curated` | Curated | 43,681      |

---

## File Structure

```
stedi-spark/
├── scripts/
│   ├── customer_landing.sql
│   ├── accelerometer_landing.sql
│   ├── step_trainer_landing.sql
│   ├── customer_trusted.py
│   ├── accelerometer_trusted.py
│   ├── step_trainer_trusted.py
│   ├── customers_curated.py
│   ├── machine_learning_curated.py
├── data/
│   ├── customer_landing/
│   ├── accelerometer_landing/
│   ├── step_trainer_landing/
├── screenshots/
│   ├── customer_landing.png
│   ├── accelerometer_landing.png
│   ├── step_trainer_landing.png
│   ├── customer_trusted.png
│   ├── machine_learning_curated.png
└── README.md
```

---

## Screenshots and Deliverables

- **Landing Zone Validation**:
  - Screenshots of `customer_landing`, `accelerometer_landing`, and `step_trainer_landing` queries in Athena.

- **Trusted Zone Validation**:
  - Screenshot of `customer_trusted` table query in Athena.

- **Curated Zone Validation**:
  - Screenshot of `machine_learning_curated` table query in Athena.

---

### Additional Notes

- Use AWS Glue Studio for visualizing workflows and debugging.
- Monitor job logs to ensure ETL processes run successfully.
- Validate row counts at each stage against expected results.

