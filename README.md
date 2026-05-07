# Air-flow-project

This project is an **Apache Airflow data pipeline** that automates daily sales revenue analysis using data from a **PostgreSQL** database.

## Project Overview

The DAG performs the following tasks:

1. **Fetches sales data** from PostgreSQL by joining:
   - `orders`
   - `order_details`
   - `products`

2. **Calculates total daily revenue** using:
   - `quantity * price`

3. **Aggregates revenue by date**

4. **Generates a time series plot** of daily sales revenue using `matplotlib`

---

## Technologies Used

- **Python**
- **Apache Airflow**
- **PostgreSQL**
- **Pandas**
- **Matplotlib**

---

## Project Structure

```bash
Air-flow-project/
│── README.md
│── daily_sales_revenue_analysis.py
