import os
import psycopg2
import pandas as pd
from google.cloud import bigquery


# Create PostgreSQL connection
def postgreSQL_connection():
    conn = psycopg2.connect(database=os.getenv('PG_DATABASE'),
                            user=os.getenv('PG_USER'), 
                            host=os.getenv('PG_HOST'),
                            password=os.getenv('PG_PASSWORD'),  
                            port=os.getenv('PG_PORT'))

    pg_conn = conn.cursor()
    return conn, pg_conn

# Extract function: Fetch data from PostgreSQL
def extract_data(**kwargs):
    conn, pg_conn = postgreSQL_connection()

    # Fetch data from ORDERS and ORDERDETAILS table
    query = """
            SELECT ors.orderid, 
                ors.orderdate, 
                ors.customername, 
                ors.state, 
                ors.city, 
                orsd.amount, 
                orsd.profit, 
                orsd.quantity, 
                orsd.category, 
                orsd.subcategory
            FROM ORDERS ors
            LEFT JOIN ORDERDETAILS orsd
                ON ors.orderid = orsd.orderid
            WHERE ors.orderdate = CURRENT_DATE - INTERVAL '1 day';            
            """
    pg_conn.execute(query)
    orders = pg_conn.fetchall()
    orders_columns = [desc[0] for desc in pg_conn.description]  # Extract column names

    # Convert to Pandas DataFrame
    order_df = pd.DataFrame(orders, columns=orders_columns)

    # Close PostgreSQL connection
    pg_conn.close()
    conn.close()

    # Push data to XCom
    kwargs['ti'].xcom_push(key='order_data', value=order_df.to_dict(orient='records'))

# Transform function: Apply necessary transformations
def transform_data(**kwargs):
    ti = kwargs['ti']
    
    # Pull data from XCom (corrected to use 'order_data')
    order_data = ti.xcom_pull(task_ids='extract_data', key='order_data')

    # Convert to DataFrame
    transformed_df = pd.DataFrame(order_data)

    # Extract year and month from 'orderdate'
    transformed_df['orderdate'] = pd.to_datetime(transformed_df['orderdate'], errors='coerce')
    transformed_df['month'] = transformed_df['orderdate'].dt.month
    transformed_df['year'] = transformed_df['orderdate'].dt.year 

    # Convert 'orderdate' to string in the format 'YYYY-MM-DD' before pushing to XCom
    transformed_df['orderdate'] = transformed_df['orderdate'].dt.strftime('%Y-%m-%d')   

    # Calculate total revenue and total profit per order
    order_summary = transformed_df.groupby('orderid').agg(total_revenue=('amount', 'sum'),
                                                      total_profit=('profit', 'sum')).reset_index()

    # Add customer segmentation (e.g., High Value or Low Value)
    order_summary['customer_segment'] = order_summary['total_revenue'].apply(lambda x: 'High Value' if x > 1000 else 'Low Value')

    # Merge the summary with the original data for a complete denormalized view
    final_df = pd.merge(transformed_df, order_summary, on='orderid', how='inner')

    # Push transformed data to XCom
    ti.xcom_push(key='transformed_data', value=final_df.to_dict(orient='records'))

# Load function: Save data to BigQuery
def load_to_bigquery(**kwargs):
    ti = kwargs['ti']
    
    # Pull transformed data from XCom
    transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')

    # Convert to DataFrame
    data_df = pd.DataFrame(transformed_data)

    # Convert 'orderdate' to date before load data into bigquery
    data_df['orderdate'] = pd.to_datetime(data_df['orderdate'], errors='coerce')

    project_id = os.getenv('BQ_PROJECT_ID')
    dataset_id = os.getenv('BQ_DATASET_ID')
    table_id = os.getenv('BQ_TABLE_ID')

    client = bigquery.Client(project=project_id)

    # Define dataset reference
    dataset_ref = client.dataset(dataset_id)

    # Ensure the dataset exists (if required)
    try:
        client.get_dataset(dataset_ref)  # Check if dataset exists
    except Exception:
        client.create_dataset(dataset_ref, exists_ok=True)

    # Define table reference correctly
    table_ref = dataset_ref.table(table_id)

    # Define job configuration
    job_config = bigquery.LoadJobConfig(
        schema=[  # BigQuery schema
            bigquery.SchemaField('orderid', 'STRING'),
            bigquery.SchemaField('orderdate', 'DATE'),
            bigquery.SchemaField('customername', 'STRING'),
            bigquery.SchemaField('state', 'STRING'),
            bigquery.SchemaField('city', 'STRING'),
            bigquery.SchemaField('amount', 'NUMERIC'),
            bigquery.SchemaField('profit', 'NUMERIC'),
            bigquery.SchemaField('quantity', 'INTEGER'),
            bigquery.SchemaField('category', 'STRING'),
            bigquery.SchemaField('subcategory', 'STRING'),
            bigquery.SchemaField('total_revenue', 'NUMERIC'),
            bigquery.SchemaField('total_profit', 'NUMERIC'),
            bigquery.SchemaField('customer_segment', 'STRING'),
            bigquery.SchemaField('month', 'INTEGER'),
            bigquery.SchemaField('year', 'INTEGER')
        ],
        write_disposition='WRITE_APPEND'  # Appends new data to the existing table
        # write_disposition='WRITE_TRUNCATE',  # Overwrites the table
    )

    # Load data into BigQuery
    load_job = client.load_table_from_dataframe(data_df, table_ref, job_config=job_config)
    load_job.result()

    print("Data loaded to BigQuery successfully")
