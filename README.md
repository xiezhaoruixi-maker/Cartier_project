# Cartier Watch Product Structure Analysis (2022–2026)

## Project Overview
This project analyzes Cartier watch product structure and pricing evolution between 2022 and 2026 by combining historical monitoring data with current product information from the official Cartier website.

Initially, the project aimed to analyze gold watch prices. However, during data exploration, we found that gold models were too few to be statistically meaningful. Therefore, the analysis focus shifted to **collection-level product structure analysis**, which provides a more stable and representative view of Cartier’s product portfolio.

The project follows a complete data analytics workflow:
- Data collection
- Data cleaning and standardization
- Feature engineering
- Dataset aggregation
- Visualization and business insight generation
- Reproducible pipeline setup (Docker)

---

## Data Sources
The project integrates multiple data sources:

- Historical monitoring dataset (BigQuery)
- Cartier official website product data (scraping)
- ECB exchange rate data
- Cartier collection launch year data (Wikipedia)
- Processed analytical datasets

---

## Project Structure
# Cartier Watch Product Structure Analysis (2022–2026)

## Project Overview
This project analyzes Cartier watch product structure and pricing evolution between 2022 and 2026 by combining historical monitoring data with current product information from the official Cartier website.

Initially, the project aimed to analyze gold watch prices. However, during data exploration, we found that gold models were too few to be statistically meaningful. Therefore, the analysis focus shifted to **collection-level product structure analysis**, which provides a more stable and representative view of Cartier’s product portfolio.

The project follows a complete data analytics workflow:
- Data collection
- Data cleaning and standardization
- Feature engineering
- Dataset aggregation
- Visualization and business insight generation
- Reproducible pipeline setup (Docker)

---

## Data Sources
The project integrates multiple data sources:

- Historical monitoring dataset (BigQuery)
- Cartier official website product data (scraping)
- ECB exchange rate data
- Cartier collection launch year data (Wikipedia)
- Processed analytical datasets

---

## Project Structure
Cartier_project/
│
├── src/
│ ├── scrape_cartier_2026.py
│ ├── feature_engineering.py
│ ├── create_collection_summary.py
│
├── data/
│ ├── raw/
│ ├── processed/
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md


---

## Team Contributions

### A — Data Preparation & Pipeline
Responsible for preparing the analytical dataset used across the project.

Main contributions:
- Cleaned and standardized the BigQuery historical monitoring dataset
- Processed key fields including price, currency, collection, and size
- Generated baseline datasets for analysis
- Built a web scraping script to collect current Cartier watch product data
- Aligned historical and current datasets using feature engineering
- Generated collection-level summary dataset (`collection_summary.csv`)
- Organized the GitHub repository structure
- Containerized the pipeline using Docker for reproducibility

Pipeline:

---

## Team Contributions

### A — Data Preparation & Pipeline
Responsible for preparing the analytical dataset used across the project.

Main contributions:
- Cleaned and standardized the BigQuery historical monitoring dataset
- Processed key fields including price, currency, collection, and size
- Generated baseline datasets for analysis
- Built a web scraping script to collect current Cartier watch product data
- Aligned historical and current datasets using feature engineering
- Generated collection-level summary dataset (`collection_summary.csv`)
- Organized the GitHub repository structure
- Containerized the pipeline using Docker for reproducibility

Pipeline:
scraping → cleaning → feature engineering → collection summary


---

### B — Data Analysis & Tableau Dashboard
Completed the data analysis workflow and dashboard development.

Key contributions:
- Transformed 6,769 raw records into an analytical dataset covering 83 products
- Built Tableau dashboards following Cartier brand visual guidelines
- Created five professional visualizations
- Revealed pricing evolution trends from 2022 to 2026
- Demonstrated how structured data can be translated into business insights

---

### C — External Data Collection
Responsible for collecting supplementary datasets.

Tasks completed:
- Scraped ECB exchange rate data using Python
- Cleaned CSV files and computed yearly average exchange rates
- Exported standardized exchange rate datasets
- Collected Cartier collection launch year data from Wikipedia mirror pages
- Parsed HTML using simulated browser requests and regex
- Exported cleaned launch-year dataset
- Filtered BigQuery historical data to retain Cartier-related records

---

### D — Visualization & Business Intelligence
Focused on transforming processed datasets into business insights.

Datasets used:
- `baseline_2022_eur.csv`
- `current_2026_labeled.csv`
- `collection_summary.csv`

Key analysis modules:
- Market structure analysis (price distribution)
- Pricing evolution analysis (matched SKUs)
- Portfolio strategy analysis (collection-level comparison)

Delivered an interactive Tableau dashboard with filtering, sorting, and drill-down capabilities.

---

## Reproducibility (Docker)

The data preparation pipeline is containerized using Docker to ensure consistent execution across environments.

### Build and run the project
```bash
docker compose up --build


### Enter the container:  docker exec -it cartier_project bash

### Example dependency check:  python -c "import pandas, requests, sklearn"

### The central dataset produced in this project is:  collection_summary.csv

This dataset aggregates:
product counts by collection
average price
median price
collection share
yearly comparison
It serves as the foundation for collection-level product structure analysis.
