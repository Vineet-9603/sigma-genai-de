import logging
import os
import shutil
import uuid
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, current_timestamp, input_file_name, lit, to_date, when
from pyspark.sql.types import DoubleType, StringType, DateType

logging.basicConfig(level=logging.INFO)

def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    try:
        logging.info("[Stage: Ingest Bronze] Starting ingestion")
        
        transactions_df = spark.read.format("delta").load(input_path + "/transactions_raw")
        promotions_df = spark.read.csv(input_path + "/promotions.csv", header=True, inferSchema=True)

        transactions_df = transactions_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                          .withColumn("source_file_name", input_file_name()) \
                                         .withColumn("batch_id", lit(str(uuid.uuid4())))
        
        promotions_df = promotions_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                     .withColumn("source_file_name", input_file_name()) \
                                    .withColumn("batch_id", lit(str(uuid.uuid4())))
        
        transactions_partition_path = f"{output_path}/bronze/transactions/transaction_date={run_date}"
        promotions_partition_path = f"{output_path}/bronze/promotions/run_date={run_date}"
        
        shutil.rmtree(transactions_partition_path, ignore_errors=True)
        shutil.rmtree(promotions_partition_path, ignore_errors=True)
        
        transactions_df.write.partitionBy("transaction_date").mode("overwrite").parquet(output_path + "/bronze/transactions")
        promotions_df.write.partitionBy("run_date").mode("overwrite").parquet(output_path + "/bronze/promotions")
        
        logging.info(f"[Stage: Ingest Bronze] Ingestion completed. Transactions: {transactions_df.count():,} rows, Promotions: {promotions_df.count():,} rows")
    except Exception as e:
        logging.error(f"[Stage: Ingest Bronze] Error: {e}")
        raise

def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    try:
        logging.info("[Stage: Transform Silver] Starting transformation")
        
        transactions_df = spark.read.parquet(bronze_path + "/bronze/transactions").where(col("transaction_date") == run_date)
        promotions_df = spark.read.parquet(bronze_path + "/bronze/promotions").where(col("run_date") == run_date)
        
        transactions_df = transactions_df.withColumn("amount", col("amount").cast(DoubleType())) \
                                         .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd").cast(DateType()))
        promotions_df = promotions_df.withColumn("discount_pct", col("discount_pct").cast(DoubleType())) \
                                    .withColumn("promo_cost", col("promo_cost").cast(DoubleType()))
        
        transactions_df = transactions_df.filter(col("amount") > 0) \
                                        .filter(col("transaction_id").isNotNull()) \
                                        .filter(col("customer_id").isNotNull())
        promotions_df = promotions_df.filter(col("discount_pct").between(0.0, 1.0)) \
                                   .filter(col("channel").isin(['Email', 'SMS', 'Push']))
        
        logging.info(f"[Stage: Transform Silver] After filtering: Transactions: {transactions_df.count():,} rows, Promotions: {promotions_df.count():,} rows")
        
        transactions_df = transactions_df.withColumn("rank", (col("ingestion_timestamp").desc()).over(Window.partitionBy("transaction_id").orderBy("ingestion_timestamp"))) \
                                       .filter(col("rank") == 1).drop("rank")
        
        logging.info(f"[Stage: Transform Silver] After deduplication: Transactions: {transactions_df.count():,} rows")
        
        promotions_df = promotions_df.hint("broadcast")
        merchants_df = spark.read.parquet(merchants_path).cache()
        joined_df = transactions_df.join(promotions_df, transactions_df.customer_id == promotions_df.customer_id, "inner")
        
        joined_df = joined_df.withColumn("quality_flag", when(col("customer_id").isNotNull() & col("channel").isin(['Email', 'SMS', 'Push']), "CLEAN").otherwise("UNMATCHED"))
        
        silver_partition_path = f"{output_path}/silver/transaction_date={run_date}"
        shutil.rmtree(silver_partition_path, ignore_errors=True)
        
        joined_df.write.partitionBy("transaction_date").mode("overwrite").parquet(output_path + "/silver")
        
        logging.info(f"[Stage: Transform Silver] Transformation completed: {joined_df.count():,} rows")
    except Exception as e:
        logging.error(f"[Stage: Transform Silver] Error: {e}")
        raise

def run_gold(spark, silver_path, gold_output_dir, run_date):
    try:
        logging.info("[Stage: Build Gold] Starting gold layer build")
        
        if not os.path.exists(gold_output_dir):
            os.makedirs(gold_output_dir)
        
        build_merchant_performance(spark, f"{silver_path}/merchants", f"{gold_output_dir}/merchant_performance", run_date)
        build_customer_ltv(spark, f"{silver_path}/customers", f"{gold_output_dir}/customer_ltv")
        build_daily_summary(spark, f"{silver_path}/transactions", f"{gold_output_dir}/daily_summary", run_date)
        
        run_metadata = {
            "run_date": run_date,
            "gold_output_dir": gold_output_dir,
            "tables_built": ["merchant_performance", "customer_ltv", "daily_summary"],
            "run_status": "SUCCESS",
            "started_at": datetime.now().isoformat(),
            "completed_at": datetime.now().isoformat()
        }
        
        with open(f"{gold_output_dir}/run_metadata_{run_date}.json", "w") as f:
            f.write(json.dumps(run_metadata, indent=4))
        
        logging.info("[Stage: Build Gold] Gold layer build completed successfully")
    except Exception as e:
        logging.error(f"[Stage: Build Gold] Error: {e}")
        run_metadata["run_status"] = "FAILED"
        run_metadata["error_message"] = str(e)
        
        with open(f"{gold_output_dir}/run_metadata_{run_date}.json", "w") as f:
            f.write(json.dumps(run_metadata, indent=4))
        
        raise

def build_merchant_performance(spark, silver_path, output_path, run_date):
    try:
        logging.info("[Stage: Build Merchant Performance] Starting build")
        
        silver_df = spark.read.format("delta").load(silver_path).where(col("date") == run_date)  # Partition pruning
        
        completed_df = silver_df.filter(col("status") == "COMPLETED")
        
        merchant_performance_df = completed_df.groupBy("merchant_id", "merchant_name", "category", "city", "date") \
            .agg(
                sum("amount").alias("total_revenue"),
                countDistinct("transaction_id").alias("txn_count"),
                (count("transaction_id") - count(when(col("status") == "COMPLETED", "transaction_id"))) / count("transaction_id") * 100
              .alias("failure_rate_pct")
            )
        
        merchant_performance_partition_path = f"{output_path}/date={run_date}"
        shutil.rmtree(merchant_performance_partition_path, ignore_errors=True)
        
        merchant_performance_df.write.partitionBy("date").mode("overwrite").delta(output_path)
        
        logging.info(f"[Stage: Build Merchant Performance] Build completed: {merchant_performance_df.count():,} rows")
    except Exception as e:
        logging.error(f"[Stage: Build Merchant Performance] Error: {e}")
        raise

def build_customer_ltv(spark, silver_path, output_path):
    try:
        logging.info("[Stage: Build Customer LTV] Starting build")
        
        silver_df = spark.read.format("delta").load(silver_path)
        
        completed_df = silver_df.filter(col("status") == "COMPLETED")
        
        customer_ltv_df = completed_df.groupBy("customer_id") \
           .agg(
                sum("amount").alias("total_spent"),
                count("transaction_id").alias("total_txns"),
                expr("AVG(amount)").alias("avg_txn_value"),
                min("transaction_date").alias("first_txn_date"),
                max("transaction_date").alias("last_txn_date"),
                mode("payment_method").alias("preferred_payment_method")
            )
        
        shutil.rmtree(output_path, ignore_errors=True)
        
        customer_ltv_df.write.mode("overwrite").delta(output_path)
        
        logging.info(f"[Stage: Build Customer LTV] Build completed: {customer_ltv_df.count():,} rows")
    except Exception as e:
        logging.error(f"[Stage: Build Customer LTV] Error: {e}")
        raise

def build_daily_summary(spark, silver_path, output_path, run_date):
    try:
        logging.info("[Stage: Build Daily Summary] Starting build")
        
        silver_df = spark.read.format("delta").load(silver_path).where(col("date") == run_date)  # Partition pruning
        
        daily_summary_df = silver_df.groupBy("date") \
           .agg(
                sum("amount").alias("total_revenue"),
                count("transaction_id").alias("total_txns"),
                countDistinct("customer_id").alias("unique_customers"),
                countDistinct("merchant_id").alias("unique_merchants"),
                (count("transaction_id") - count(when(col("status") == "COMPLETED", "transaction_id"))) / count("transaction_id") * 100
                .alias("failure_rate_pct")
            )
        
        daily_summary_df = daily_summary_df.withColumn("run_date", lit(run_date))
        
        daily_summary_partition_path = f"{output_path}/date={run_date}"
        shutil.rmtree(daily_summary_partition_path, ignore_errors=True)
        
        daily_summary_df.write.partitionBy("date").mode("overwrite").delta(output_path)
        
        logging.info(f"[Stage: Build Daily Summary] Build completed: {daily_summary_df.count():,} rows")
    except Exception as e:
        logging.error(f"[Stage: Build Daily Summary] Error: {e}")
        raise

def main():
    spark = SparkSession.builder.appName("Marketing Attribution Pipeline").getOrCreate()
    
    input_path = "/path/to/input"
    output_path = "/path/to/output"
    merchants_path = "/path/to/merchants"
    run_date = "2023-10-01"
    run_id = str(uuid.uuid4())
    
    ingest_bronze(spark, input_path, output_path, run_date, run_id)
    transform_silver(spark, output_path, merchants_path, output_path, run_date)
    run_gold(spark, output_path, output_path, run_date)

if __name__ == "__main__":
    main()
