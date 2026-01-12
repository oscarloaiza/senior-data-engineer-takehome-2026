# Stryker Senior Data Engineer Challenge - OpenWeather Data Pipeline

## Project Overview
This repository contains a robust data pipeline designed to ingest, model, and store weather data from the OpenWeatherMap API. The system is built with a focus on **idempotency**, **data quality**, and **scalability**, simulating a production-ready environment for downstream analytics and data science consumers.

---

## Time Spent

Total Approximate Time: 4.5 hours.

**Breakdown:**

**Research & Learning:** 2 hours (Learning how Airflow works, how tasks are managed, and how to connect to the database inside Docker).

**Data Ingestion (Fetcher) Development:** 1.5 hours (API client implementation, staging table logic, and error handling).

**Data Transformation (Transformer) Development:** 0.5 hours (Developing SQL logic for JSONB extraction and data modeling).

**Finalization & Documentation:** 0.5 hours (Git version control, code commenting, and completing the README documentation).

---

## Use of Public Resources

Although the Airflow environment and basic DAG structures were already provided, I decided to spend time researching and understanding the technology. Rather than simply inserting code into a platform I had never used before, I wanted to ensure I fully understood how the components connected and how the different tasks connect and work together.

Therefore, my reflection is that, since I had time to complete the exercise, I prioritized building a solid foundation of knowledge about how Airflow works. This approach allowed me to verify that my solution was not only functional but also aligned with the platform's best practices. This investment ensures that, in future projects, I can navigate and use this technology with greater confidence and efficiency, while also saving time on research.

I consulted the following resources for research:

- [Official Apache Airflow Docummentation](https://airflow.apache.org/docs/)
- [Astronomer.io](https://www.astronomer.io/docs/learn/overview)
- [Postgres Documentation](https://www.postgresql.org/docs/current/index.html)

---

## Assumptions

* **Network Connectivity:** I assumed that the Airflow container had internet access to connect to the OpenWeatherMap API and communicate internally with the Postgres service through the `postgres` host.
* **Data Uniqueness:** Is assumed that the extraction timestamp is sufficient to identify new records and prevent duplicating data during the transformation process."
* **Environment Setup:** I assumed that the Postgres database is configured with the default credentials (`airflow/airflow`) and that the `postgres_default` connection is already defined in Airflow.

---

## Tradeoffs & Design Decisions

* **SQL for Transformations:** I chose to do the data cleaning directly in the database using SQL instead of Python. This is faster and more efficient because the data doesn't have to move back and forth between Airflow and the database.
* **Using a Staging Table:** I created a temporary table (staging_weather) to hold the data before moving it to the permanent history. This acts as a safety step to make sure only good data is saved.
* **Preventing Duplicates:** I added a filter in the SQL code (the WHERE clause) to check for timestamps. This ensures that even if the process runs multiple times, the same data won't be saved twice.

---

## Next Steps / Improvements

If I had more time to evolve this project into a full-scale production environment, I would implement the following:

- **Testing Strategy:** I would add unit tests for the transformation logic and integration tests to ensure the API response matches our expected schema before processing.
- **Observability & Monitoring**: Implement Slack or Email alerts within the Airflow DAGs to notify the team immediately if an API call fails or if the transformation task encounters an error.
- **Data Quality Checks:** Integrate a tool like **'Great Expectations'** to automatically validate that temperatures are within a logical range and that no critical fields are missing.
---

## Instructions to the Evaluator

1. **API Key Setup:** I registered a free account at OpenWeatherMap to obtain an API Key. This key is included in the `fetcher.py` file to allow the pipeline to run immediately.
2. **Connection Setup:** Ensure the connection `postgres_default` is configured in the Airflow UI (Admin -> Connections) with host `postgres` and credentials `airflow/airflow`.
3. **Execution:** 
  * First, trigger the **`stryker_weather_fetcher_v1`** DAG to create tables and pull initial data.
  * Once it finishes, trigger the **`stryker_weather_transformer_v1`** DAG to process and clean the data.
4. **Verification:** Query the `current_weather` table in Postgres to see the final structured data.

---
