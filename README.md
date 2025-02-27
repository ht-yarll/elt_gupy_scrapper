# Gupy Jobs ETL Pipeline 📡

## 📌 Overview
This project is an **ELT (Extract, Load, Transform) pipeline** that processes *job listing data* from Gupy, categorizes it, and loads it into **Google BigQuery** for analysis. The goal is to enable **efficient job data analysis and visualization** using a BI tool such as Looker Studio.

## 🏗️ Architecture
1. **Extract**: Fetches job listings from the Gupy URL.
2. **Transform**: Cleans and enriches the data by mapping job categories, states, and vacancy types.
3. **Load**: Stores the processed data in **Google BigQuery** for further analysis.

## 🛠️ Tech Stack
- **Python** 🐍 - Main programming language ([Python](https://www.python.org/))
- **Google Cloud Storage (GCS)** ☁️ - Stores raw job data ([GCS](https://cloud.google.com/storage))
- **BigQuery** 📊 - Data warehouse for structured storage ([BigQuery](https://cloud.google.com/bigquery))
- **Apache Airflow (Astro)** 🌬️ - Orchestrates the ELT workflow ([Astronomer](https://www.astronomer.io/))
- **Looker Studio** 📈 - Data visualization and dashboarding ([Looker Studio](https://lookerstudio.google.com/))


## 📜 SQL Transformations
The **BigQuery SQL script** performs the following transformations:
- Maps **state names** to abbreviations (e.g., `São Paulo` → `SP`)
- Categorizes **job positions** (e.g., `Desenvolvedor` → `TI & Desenvolvimento`)
- Maps **vacancy types** (e.g., `vacancy_type_remote` → `Remoto`)
- Cleans and loads data into a medalion structured base
    1. Bronze 🟤 – Raw data, ingested as-is from the source.
    2. Silver ⚪ – Cleaned and structured data, often with some transformations.
    3. Gold 🟡 – Curated, aggregated, and ready-for-analysis data

## 🚀 How to Run the Pipeline
1. **Setup Google Cloud**:
   - Create a **GCS bucket** for storage.
   - Set up a **BigQuery dataset**.

2. **Create your GCP connection following Astronomer Doc**
    - [Connection doc for BQ](https://www.astronomer.io/docs/learn/connections/bigquery/?tab=key-file-value#bigquery-connection)

4. **Orchestrate with Airflow**:
   - Start Airflow:
     ```bash
     astro dev start
     ```
   - Trigger the DAG manually or schedule it.
--------
> Ps.: I strongly recommend reading the following for a better understanding of the whole process.
> 1. 🧮[BigQuery Doc](https://cloud.google.com/bigquery/docs?authuser=1)
> 2. ☁️📦[CloudStorage Doc](https://cloud.google.com/storage/docs?authuser=1)
> 3. 💫[Astronomer Doc](https://www.astronomer.io/docs/) 

## 📊 Dashboard
After processing, the data is available in **BigQuery** and can be visualized using **Looker Studio**. The dashboard includes:
- Job distribution by category
- Remote vs. onsite job trends

[Dashboard]()

## 🤝 Contributing
Feel free to submit pull requests or suggest improvements! 😊

## 📄 License
This project is open-source under the **MIT License**.

---
💡 *Happy coding!* 😃


## Author
[@ht-yarll](https://github.com/ht-yarll) *AKA me!*

