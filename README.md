## Airbnb Listings Data Pipeline with Snowflake, DBT, and Python

## 📘 Project Overview
This project demonstrates a complete data pipeline for modeling Airbnb listings data. It leverages Python for ETL, Snowflake for data warehousing, and DBT for data modeling. The raw data is sourced from an open-source Airbnb listings dataset, and the architecture is designed to be scalable and automated.

## 🛠️ Tech Stack
- **Python**: For extracting and transforming the raw CSV data.
- **AWS S3**: Storage for raw and processed data.
- **Snowflake**: Cloud data platform for warehousing and querying data.
- **Snowpipe**: Automated data ingestion via file notifications.
- **DBT (Data Build Tool)**: For transforming and modeling data.

## 🔧 Project Workflow
1. **Data Extraction:** Python scripts download raw Airbnb listings CSV data to a local directory.
2. **Data Upload:** The raw CSV files are uploaded to an S3 bucket.
3. **Automated Ingestion:** S3 file notifications trigger Snowpipe, automatically ingesting the data into raw tables in Snowflake.
4. **Data Modeling:** DBT models transform the raw data into analysis-ready tables.


![Screenshot 2025-03-03 084612](https://github.com/user-attachments/assets/94d93d0d-4aa1-45ed-95f2-ccf26e915562)

![Screenshot 2025-03-08 055237](https://github.com/user-attachments/assets/152896d9-21be-4b99-a785-fa12f7cd51dc)
