from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
import logging

default_args = {
    'owner': 'data-engineering',
   'retries': 2,
   'retry_delay': timedelta(minutes=5),
    'email_on_failure': True
}

def on_failure_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    error_message = context['exception']
    logging.error(f"DAG: {dag_id}, Task: {task_id}, Execution Date: {execution_date}, Error: {error_message}")

def sla_miss_callback(context):
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    logging.warning(f"DAG: {dag_id}, Execution Date: {execution_date} missed SLA")

def extract_bronze(**context):
    spark = SparkSession.builder.appName("Bronze Extraction").getOrCreate()
    logging.info(f"Starting Bronze Extraction for {context['execution_date']}")
    try:
        df_transactions = spark.read.csv('transactions.csv', header=True, inferSchema=True)
        df_merchants = spark.read.csv('merchants.csv', header=True, inferSchema=True)
        df_transactions = df_transactions.withColumn('ingestion_timestamp', current_timestamp()) \
                                         .withColumn('source_file', lit('transactions.csv')) \
                                        .withColumn('pipeline_run_id', current_timestamp())
        df_merchants = df_merchants.withColumn('ingestion_timestamp', current_timestamp()) \
                                   .withColumn('source_file', lit('merchants.csv')) \
                                   .withColumn('pipeline_run_id', current_timestamp())
        df_transactions.write.mode('overwrite').parquet('bronze/transactions/')
        df_merchants.write.mode('overwrite').parquet('bronze/merchants/')
        logging.info(f"Completed Bronze Extraction for {context['execution_date']}")
    except Exception as e:
        logging.error(f"Bronze Extraction failed: {e}")
        raise

def transform_silver(**context):
    spark = SparkSession.builder.appName("Silver Transformation").getOrCreate()
    logging.info(f"Starting Silver Transformation for {context['execution_date']}")
    try:
        df_transactions = spark.read.parquet('bronze/transactions/')
        df_merchants = spark.read.parquet('bronze/merchants/')
        df_transactions = df_transactions.withColumn('amount', col('amount').cast('float')) \
                                        .withColumn('transaction_date', col('transaction_date').cast('date')) \
                                       .withColumn('transaction_id', col('transaction_id').cast('string')) \
                                       .withColumn('merchant_id', col('merchant_id').cast('string'))
        df_transactions = df_transactions.filter((col('transaction_id').isNotNull()) & (col('amount') >= 0))
        df_transactions = df_transactions.join(df_merchants, df_transactions.merchant_id == df_merchants.merchant_id, 'left') \
                                        .withColumn('quality_flag', when(col('merchant_id').isNull(), 'UNMATCHED').otherwise('MATCHED'))
        df_transactions = df_transactions.dropDuplicates(['transaction_id', 'ingestion_timestamp']) \
                                        .select(['transaction_id', 'amount', 'transaction_date','merchant_id', 'quality_flag'] + [c for c in df_transactions.columns if c not in ['transaction_id', 'amount', 'transaction_date','merchant_id', 'quality_flag']])
        df_transactions.write.mode('overwrite').parquet('silver/')
        logging.info(f"Completed Silver Transformation for {context['execution_date']}")
    except Exception as e:
        logging.error(f"Silver Transformation failed: {e}")
        raise

def build_gold(**context):
    spark = SparkSession.builder.appName("Gold Aggregation").getOrCreate()
    logging.info(f"Starting Gold Aggregation for {context['execution_date']}")
    try:
        df_silver = spark.read.parquet('silver/')
        df_silver = df_silver.withColumn('status', when(col('status') == 'COMPLETED', col('amount')).otherwise(lit(0)))
        df_merchant_performance = df_silver.groupBy('merchant_id','merchant_name', 'category', 'city', 'transaction_date') \
                                           .agg({'status':'sum', 'transaction_id': 'count', 'amount': 'avg'}) \
                                          .withColumnRenamed('sum(status)', 'total_revenue') \
                                          .withColumn('txn_count', col('count(transaction_id)')) \
                                          .withColumn('failure_rate_pct', (col('count(transaction_id)') - col('sum(status)')) / col('count(transaction_id)') * 100)
        df_customer_ltv = df_silver.groupBy('customer_id') \
                                   .agg({'status':'sum', 'transaction_id': 'count', 'amount': 'avg'}) \
                                   .withColumnRenamed('sum(status)', 'total_spent') \
                                  .withColumn('total_txns', col('count(transaction_id)')) \
                                  .withColumn('avg_txn_value', col('avg(amount)')) \
                                  .withColumn('first_txn_date', first('transaction_date')) \
                                  .withColumn('last_txn_date', last('transaction_date')) \
                                  .withColumn('preferred_payment_method', lit('N/A'))
        df_daily_summary = df_silver.groupBy('transaction_date') \
                                     .agg({'status':'sum', 'transaction_id': 'count', 'amount': 'avg'}) \
                                     .withColumnRenamed('sum(status)', 'total_revenue') \
                                     .withColumn('total_txns', col('count(transaction_id)')) \
                                     .withColumn('unique_customers', lit(None)) \
                                     .withColumn('unique_merchants', lit(None)) \
                                     .withColumn('failure_rate_pct', (col('count(transaction_id)') - col('sum(status)')) / col('count(transaction_id)') * 100)
        df_merchant_performance.write.mode('overwrite').parquet('gold/merchant_performance/')
        df_customer_ltv.write.mode('overwrite').parquet('gold/customer_ltv/')
        df_daily_summary.write.mode('overwrite').parquet('gold/daily_summary/')
        logging.info(f"Completed Gold Aggregation for {context['execution_date']}")
    except Exception as e:
        logging.error(f"Gold Aggregation failed: {e}")
        raise

with DAG(
    dag_id='sigma_transaction_pipeline',
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    on_failure_callback=on_failure_callback,
    sla_miss_callback=sla_miss_callback,
    tags=['sigma', 'transactions', 'daily'],
    description="Daily Bronze->Silver->Gold pipeline for Sigma DataTech transactions"
) as dag:

    extract_bronze = PythonOperator(
        task_id='extract_bronze',
        python_callable=extract_bronze,
        on_failure_callback=on_failure_callback
    )

    transform_silver = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_silver,
        on_failure_callback=on_failure_callback
    )

    build_gold = PythonOperator(
        task_id='build_gold',
        python_callable=build_gold,
        on_failure_callback=on_failure_callback
    )

    extract_bronze >> transform_silver >> build_gold
