# Valuation Engine

This repository is the data modelling part of the https://dashboard-finance-metrics.vercel.app/. It focuses on extracting financial data from the open-source SEC Financial Statement Data Sets, transforming data into data models with dynamic data pipelines, analyzing and calculating key financial metrics, and loading the processed data into a cloud database. The data is then used in another repository to create an interactive website platform for data visualization.

## Features
- Data Extraction: Utilizes Python scripts to efficiently extract financial data from SEC Financial Statement Data Sets.
- Data Transformation and Analytics: Transforms data and calculate key financial metrics.
- Cloud Database Integration: Loads the processed data into a cloud database for scalability and accessibility.

## Technologies Used
- SEC Financial Statement Data Sets (https://www.sec.gov/dera/data/financial-statement-data-sets)
- Python for ETL processes and data analytics
- Local and Cloud MYSQL database for data storage and management

## Getting Start
- To get started with this project, clone the repository
- Configure .venv Python environment
- Install the required python libraries: python-dotenv, mysql-connector-python, pandas, zipfile36, openpyxl
- You would need .env file with proper MySQL database connection
- The key file is <b>run_engine.py</b> 

## License
This project is licensed under the MIT License - see the LICENSE file for details.
