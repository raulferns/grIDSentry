import os
import glob
import time
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType
from src.config import CICIDS_RAW_DIR, PARQUET_DIR, FEATURE_COLS
from src.spark_session import get_spark_session

TRAFFIC_DIR = os.path.join(CICIDS_RAW_DIR, "TrafficLabelling")

def get_raw_dataset_files():
    """Returns the 1.12 GB real-world PCAP flow CSV files."""
    csv_files = glob.glob(os.path.join(TRAFFIC_DIR, "*.csv"))
    if not csv_files:
        # Fallback to any CSVs in CICIDS_RAW_DIR
        csv_files = [
            os.path.join(root, f)
            for root, _, files in os.walk(CICIDS_RAW_DIR)
            for f in files if f.endswith(".csv")
        ]
    return csv_files

def ingest_cicids_to_parquet(spark=None, force_reload=False):
    """
    Ingests 1.12 GB of real-world CICIDS2017 network traffic CSVs,
    cleans whitespace column headers, normalizes types, maps attacks, and writes to Parquet.
    """
    if spark is None:
        spark = get_spark_session()

    parquet_path = os.path.join(PARQUET_DIR, "cicids_flows.parquet")

    if os.path.exists(parquet_path) and not force_reload:
        print(f"[CACHE] Loading cached columnar Parquet dataset from: {parquet_path}")
        df = spark.read.parquet(parquet_path)
        return df

    csv_files = get_raw_dataset_files()
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {TRAFFIC_DIR}. Run download_cicids17.py first.")

    total_csv_bytes = sum(os.path.getsize(f) for f in csv_files)
    print(f"\n[INGESTION] Found {len(csv_files)} real-world CSV files ({total_csv_bytes / (1024*1024*1024):.3f} GB / {total_csv_bytes / (1024*1024):.1f} MB)")
    print("[INGESTION] Reading distributed raw CSVs into Spark...")
    t0 = time.time()

    raw_df = spark.read.option("header", "true").option("inferSchema", "false").csv(csv_files)

    # Clean whitespace from column names
    col_mapping = {}
    for c in raw_df.columns:
        col_mapping[c] = c.strip()

    for orig_c, clean_c in col_mapping.items():
        if orig_c != clean_c:
            raw_df = raw_df.withColumnRenamed(orig_c, clean_c)

    name_map = {
        "Source IP": "src_ip",
        "Source Port": "src_port",
        "Destination IP": "dst_ip",
        "Destination Port": "dst_port",
        "Protocol": "protocol",
        "Flow Duration": "flow_duration",
        "Total Fwd Packets": "total_fwd_packets",
        "Total Backward Packets": "total_bwd_packets",
        "Total Length of Fwd Packets": "total_len_fwd_pkts",
        "Total Length of Bwd Packets": "total_len_bwd_pkts",
        "Fwd Packet Length Max": "fwd_pkt_len_max",
        "Fwd Packet Length Min": "fwd_pkt_len_min",
        "Fwd Packet Length Mean": "fwd_pkt_len_mean",
        "Fwd Packet Length Std": "fwd_pkt_len_std",
        "Bwd Packet Length Max": "bwd_pkt_len_max",
        "Bwd Packet Length Min": "bwd_pkt_len_min",
        "Bwd Packet Length Mean": "bwd_pkt_len_mean",
        "Bwd Packet Length Std": "bwd_pkt_len_std",
        "Flow Bytes/s": "flow_bytes_s",
        "Flow Packets/s": "flow_pkts_s",
        "Flow IAT Mean": "flow_iat_mean",
        "Flow IAT Std": "flow_iat_std",
        "Flow IAT Max": "flow_iat_max",
        "Flow IAT Min": "flow_iat_min",
        "Fwd IAT Mean": "fwd_iat_mean",
        "Bwd IAT Mean": "bwd_iat_mean",
        "Fwd PSH Flags": "fwd_psh_flags",
        "Fwd URG Flags": "fwd_urg_flags",
        "Fwd Header Length": "fwd_header_len",
        "Bwd Header Length": "bwd_header_len",
        "Fwd Packets/s": "fwd_pkts_s",
        "Bwd Packets/s": "bwd_pkts_s",
        "Min Packet Length": "pkt_len_min",
        "Max Packet Length": "pkt_len_max",
        "Packet Length Mean": "pkt_len_mean",
        "Packet Length Std": "pkt_len_std",
        "Packet Length Variance": "pkt_len_var",
        "FIN Flag Count": "fin_flag_cnt",
        "SYN Flag Count": "syn_flag_cnt",
        "RST Flag Count": "rst_flag_cnt",
        "PSH Flag Count": "psh_flag_cnt",
        "ACK Flag Count": "ack_flag_cnt",
        "URG Flag Count": "urg_flag_cnt",
        "Down/Up Ratio": "down_up_ratio",
        "Average Packet Size": "avg_pkt_size",
        "Init_Win_bytes_forward": "init_win_bytes_fwd",
        "Init_Win_bytes_backward": "init_win_bytes_bwd",
        "Active Mean": "active_mean",
        "Idle Mean": "idle_mean",
        "Label": "raw_label"
    }

    for orig_k, new_k in name_map.items():
        if orig_k in raw_df.columns:
            raw_df = raw_df.withColumnRenamed(orig_k, new_k)

    # Cast numeric feature columns using try_cast / null-safe float conversion
    for col_name in FEATURE_COLS:
        if col_name in raw_df.columns:
            raw_df = raw_df.withColumn(
                col_name,
                F.coalesce(
                    F.when(F.col(col_name).isin(["Infinity", "NaN", "null", "", " "]), 0.0)
                     .otherwise(F.expr(f"try_cast({col_name} AS DOUBLE)")),
                    F.lit(0.0)
                )
            )

    if "dst_port" in raw_df.columns:
        raw_df = raw_df.withColumn(
            "dst_port",
            F.coalesce(F.expr("try_cast(dst_port AS INT)"), F.lit(80))
        )

    # Clean label and assign macro threat families
    clean_label_col = F.trim(F.col("raw_label"))
    cat_expr = (
        F.when(clean_label_col == "BENIGN", "Normal")
         .when(clean_label_col.contains("DDoS") | clean_label_col.contains("DoS") | clean_label_col.contains("Heartbleed"), "DoS/DDoS")
         .when(clean_label_col.contains("PortScan"), "PortScan/Probe")
         .when(clean_label_col.contains("Patator") | clean_label_col.contains("Web Attack"), "Brute Force/Web")
         .when(clean_label_col.contains("Bot"), "Botnet")
         .when(clean_label_col.contains("Infiltration"), "Infiltration")
         .otherwise("Other Attack")
    )

    enriched_df = (
        raw_df
        .withColumn("clean_label", clean_label_col)
        .withColumn("attack_category", cat_expr)
        .withColumn("is_attack", F.when(F.col("attack_category") == "Normal", 0).otherwise(1))
    )

    print("[STORAGE] Writing optimized Snappy-compressed Apache Parquet format...")
    enriched_df.write.mode("overwrite").parquet(parquet_path)
    t_write = time.time() - t0
    print(f"[SUCCESS] Ingestion & Parquet Lakehouse created in {t_write:.2f}s at: {parquet_path}")

    # Benchmark storage
    benchmark_storage(parquet_path, total_csv_bytes)

    df_parquet = spark.read.parquet(parquet_path)
    return df_parquet

def benchmark_storage(parquet_path, total_csv_bytes):
    """Computes exact storage compression savings of Parquet vs Raw CSV."""
    parquet_bytes = sum(
        os.path.getsize(os.path.join(root, f))
        for root, _, files in os.walk(parquet_path)
        for f in files
    )
    savings = (1.0 - (parquet_bytes / total_csv_bytes)) * 100

    print("\n" + "="*65)
    print("      REAL-WORLD BIG DATA STORAGE BENCHMARK (1.12 GB CSV VS PARQUET)")
    print("="*65)
    print(f"  Raw Real-World CSV Size: {total_csv_bytes / (1024*1024*1024):.3f} GB ({total_csv_bytes / (1024*1024):.1f} MB)")
    print(f"  Columnar Parquet Size:   {parquet_bytes / (1024*1024*1024):.3f} GB ({parquet_bytes / (1024*1024):.1f} MB)")
    print(f"  Storage Reduction:       {savings:.2f}% space savings via Snappy Parquet")
    print("="*65 + "\n")

    return {
        "csv_size_mb": round(total_csv_bytes / (1024*1024), 2),
        "parquet_size_mb": round(parquet_bytes / (1024*1024), 2),
        "compression_savings_pct": round(savings, 2)
    }

if __name__ == "__main__":
    spark = get_spark_session("CICIDS_Ingestion_Test")
    df = ingest_cicids_to_parquet(spark, force_reload=True)
    print("Ingestion complete. Total flows:", df.count())
