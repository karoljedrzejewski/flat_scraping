# 🏙️ Gdańsk Flat Scraper & Analyzer

A data pipeline project for **scraping apartment listings in Gdańsk** and **analyzing prices using PySpark**. Built with **Airflow**, **Docker**, and optionally integrated with **GCP (BigQuery, GCS)** for production deployments.

---

## 🚀 What does the project do?

This project automates the process of:

1. **Scraping** listings from:
   - `OLX.pl`
   - `ogloszenia.trojmiasto.pl`
   - (more sources can be added)

2. **Cleaning & filtering** the data:
   - filters out listings with:
     - area smaller than 30 m²
     - or price per m² lower than a threshold (`PRICE_THRESH`)

3. **Saving raw and cleaned data** into CSV format

4. **Analyzing** with PySpark:
   - average price per location
   - average price per m²
   - top 5 largest and smallest flats
   - number of offers per location
   - median and mean price per m²

---

## ⚙️ Technologies used

- Python 3.10+
- Apache Airflow
- Apache Spark (PySpark)
- Docker & Docker Compose
- Pandas, BeautifulSoup

---

## ▶️ How to run the project

### 1. Clone the repo

git clone https://github.com/your_username/flat_scraping.git

### 2. Start Airflow with Docker Compose

docker-compose up --build

Airflow will be available at http://localhost:8080