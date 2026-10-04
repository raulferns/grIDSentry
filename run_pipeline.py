import json
import os
import sys
import time

ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.spark_session import get_spark_session
from src.data_ingestion import ingest_cicids_to_parquet, benchmark_storage, get_raw_dataset_files
from src.spark_sql_analytics import run_spark_sql_analytics
from src.feature_engineering import prepare_data_splits
from src.models.supervised import train_all_models
from src.models.anomaly_detection import train_kmeans_anomaly_detector
from src.evaluation import compare_all_models
from src.streaming_simulator import run_structured_streaming_detection
from src.config import OUTPUTS_DIR, PARQUET_DIR

def run_full_bda_pipeline(force_reload_parquet=False):
    start_total_time = time.time()
    print("\n" + "#"*75)
    print("   BIG DATA ANALYTICS: 1.94 GB REAL-WORLD NETWORK INTRUSION DETECTION")
    print("   Dataset: Canadian Institute for Cybersecurity (CICIDS2017)")
    print("   Engine: Apache Spark 4.2 / PySpark | Storage: Snappy Parquet")
    print("#"*75 + "\n")

    spark = get_spark_session("CICIDS_RealWorld_Pipeline")
    print(f"[STAGE 1/7] SparkSession Active (Spark version: {spark.version})")

    # Ingestion & Parquet conversion of 1.94 GB raw CSV dataset
    print("\n[STAGE 2/7] Ingesting 1.94 GB Real-World Network Flows & Building Parquet Lakehouse...")
    parquet_path = os.path.join(PARQUET_DIR, "cicids_flows.parquet")
    df = ingest_cicids_to_parquet(spark, force_reload=force_reload_parquet)
    total_records = df.count()
    print(f"  -> Total Real-World Network Flows Processed: {total_records:,}")

    # Compute storage footprint benchmark
    csv_files = get_raw_dataset_files()
    total_csv_bytes = sum(os.path.getsize(f) for f in csv_files)
    storage_bench = benchmark_storage(parquet_path, total_csv_bytes)

    # Spark SQL Analytics on real-world dataset
    print("\n[STAGE 3/7] Running Distributed Spark SQL Threat Analytics...")
    sql_analytics = run_spark_sql_analytics(df, spark, export_json=True)

    # Distributed Train / Test Split for Spark MLlib
    print("\n[STAGE 4/7] Constructing Distributed Feature Engineering Pipeline...")
    # Sample a representative slice of 60,000 flows for fast, accurate local ML training
    sample_df = df.sample(withReplacement=False, fraction=min(1.0, 60000.0 / total_records), seed=42)
    train_df, test_df = sample_df.randomSplit([0.75, 0.25], seed=42)
    train_count = train_df.count()
    test_count = test_df.count()
    print(f"  -> ML Training Partition: {train_count:,} flows")
    print(f"  -> ML Testing Partition:  {test_count:,} flows")

    train_transformed, test_transformed, feature_pipeline = prepare_data_splits(
        train_df, test_df, is_multiclass=False
    )

    # Supervised Classification Models
    print("\n[STAGE 5/7] Training Distributed Supervised Classification Models...")
    models, train_times = train_all_models(train_transformed, is_multiclass=False)

    # Model Evaluation & Benchmark Comparison
    print("\n[STAGE 6/7] Evaluating Classification Models on Test Partitions...")
    model_benchmarks = compare_all_models(models, test_transformed, train_times, is_multiclass=False)

    # Unsupervised Anomaly Detection with K-Means
    print("\n[STAGE 6b/7] Running Unsupervised Anomaly Detection (K-Means Centroid Distance)...")
    kmeans_model, scored_test, kmeans_metrics = train_kmeans_anomaly_detector(train_transformed, test_transformed, k=6)

    # Real-Time Structured Streaming Simulation
    print("\n[STAGE 7/7] Simulating Real-World Flow Stream & Intrusion Detection...")
    run_structured_streaming_detection(spark, runtime_seconds=6)

    total_pipeline_duration = time.time() - start_total_time
    print("\n" + "="*75)
    print(f"   BIG DATA PIPELINE COMPLETED SUCCESSFULLY IN {total_pipeline_duration:.2f} SECONDS")
    print("="*75)

    # Dynamic Dashboard Data Export
    dashboard_data = {
        "pipeline_metadata": {
            "spark_version": spark.version,
            "dataset_name": "CICIDS2017 Real-World Benchmark",
            "total_records_ingested": total_records,
            "train_records": train_count,
            "test_records": test_count,
            "pipeline_runtime_sec": round(total_pipeline_duration, 2)
        },
        "storage_benchmark": storage_bench,
        "sql_analytics": sql_analytics,
        "model_benchmarks": model_benchmarks,
        "unsupervised_kmeans": kmeans_metrics
    }

    dashboard_out = os.path.join(OUTPUTS_DIR, "dashboard_data.json")
    with open(dashboard_out, "w") as f:
        json.dump(dashboard_data, f, indent=2, default=float)
    print(f"[EXPORT] Consolidated dashboard JSON written to: {dashboard_out}")

    dashboard_js = os.path.join(ROOT_DIR, "dashboard", "dashboard_data.js")
    with open(dashboard_js, "w") as f:
        f.write("window.DASHBOARD_DATA = " + json.dumps(dashboard_data, indent=2, default=float) + ";")
    print(f"[EXPORT] Direct client JS data updated: {dashboard_js}\n")

    return dashboard_data

if __name__ == "__main__":
    run_full_bda_pipeline(force_reload_parquet=False)
