from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta
import pandas as pd
import matplotlib.pyplot as plt
import os

PG_CONN_ID = 'postgres_conn'
OUTPUT_DIR = '/home/kiwilytics/airflow_output'

default_args = {
    'owner': 'kiwilytics',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def fetch_order_data():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()

    query = """
    SELECT
        o.OrderDate::date AS sale_date,
        od.ProductID AS product_id,
        p.ProductName AS product_name,
        od.Quantity AS quantity,
        p.Price AS price
    FROM orders o
    JOIN order_details od ON o.OrderID = od.OrderID
    JOIN products p ON od.ProductID = p.ProductID
    """

    df = pd.read_sql(query, conn)
    df.to_csv(f'{OUTPUT_DIR}/daily_sales_data.csv', index=False)
    print("Sales data fetched successfully.")

def process_daily_revenue():
    df = pd.read_csv(f'{OUTPUT_DIR}/daily_sales_data.csv')

    df['total_revenue'] = df['quantity'] * df['price']

    revenue_per_day = (
        df.groupby('sale_date')
        .agg(total_revenue=('total_revenue', 'sum'))
        .reset_index()
    )

    revenue_per_day.to_csv(f'{OUTPUT_DIR}/daily_revenue.csv', index=False)
    print("Daily revenue calculated successfully.")

def plot_daily_revenue():
    df = pd.read_csv(f'{OUTPUT_DIR}/daily_revenue.csv')
    df['sale_date'] = pd.to_datetime(df['sale_date'])

    plt.figure(figsize=(12, 6))
    plt.plot(df['sale_date'], df['total_revenue'], marker='o', linestyle='-')
    plt.title("Daily Total Sales Revenue")
    plt.xlabel("Date")
    plt.ylabel("Total Revenue")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()

    output_path = f'{OUTPUT_DIR}/daily_revenue_plot.png'
    plt.savefig(output_path)
    plt.close()

    print(f"Revenue chart saved to {output_path}")

with DAG(
    dag_id='daily_sales_revenue_analysis',
    default_args=default_args,
    description='Fetch sales data, calculate daily revenue, and plot it',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    tags=['sales', 'postgres', 'analytics'],
) as dag:

    task_fetch_data = PythonOperator(
        task_id='fetch_order_data',
        python_callable=fetch_order_data
    )

    task_process_revenue = PythonOperator(
        task_id='process_daily_revenue',
        python_callable=process_daily_revenue
    )

    task_plot_revenue = PythonOperator(
        task_id='plot_daily_revenue',
        python_callable=plot_daily_revenue
    )

    task_fetch_data >> task_process_revenue >> task_plot_revenue