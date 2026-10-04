import json
import os
import shutil
import time
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, LongType
)
from src.config import STREAMING_DIR, PARQUET_DIR, OUTPUTS_DIR
from src.spark_session import get_spark_session

STREAM_INPUT_DIR = os.path.join(STREAMING_DIR, "input_stream")
STREAM_CHECKPOINT_DIR = os.path.join(STREAMING_DIR, "checkpoints")
STREAM_OUTPUT_DIR = os.path.join(STREAMING_DIR, "alerts")

def reset_streaming_directories():
    """Cleans up previous stream files and checkpoint state."""
    for path in [STREAM_INPUT_DIR, STREAM_CHECKPOINT_DIR, STREAM_OUTPUT_DIR]:
        if os.path.exists(path):
            shutil.rmtree(path, ignore_errors=True)
        os.makedirs(path, exist_ok=True)

def simulate_real_world_streaming(spark, num_batches=3, batch_size=200):
    """
    Extracts real-world packet flow records from the Parquet Lakehouse and stages them
    as micro-batches in the streaming directory.
    Also exports sample records to outputs/streaming_packets.json for the web dashboard.
    """
    parquet_path = os.path.join(PARQUET_DIR, "cicids_flows.parquet")
    if not os.path.exists(parquet_path):
        print("[WARN] Parquet data not found for streaming simulation.")
        return

    df = spark.read.parquet(parquet_path)
    
    # Stratified-style sample with both Normal and Attack flows
    normal_sample = df.filter(F.col("is_attack") == 0).limit(int(num_batches * batch_size * 0.7))
    attack_sample = df.filter(F.col("is_attack") == 1).limit(int(num_batches * batch_size * 0.3))
    sample_df = normal_sample.union(attack_sample)

    selected_cols = ["dst_port", "flow_duration", "total_fwd_packets", "total_len_fwd_pkts", 
                     "is_attack", "attack_category", "clean_label"]
    
    records = [r.asDict() for r in sample_df.select(*selected_cols).collect()]

    # Export realistic packet stream for dashboard
    export_packets = []
    for r in records[:150]:
        export_packets.append({
            "dst_port": r.get("dst_port", 80),
            "flow_duration_us": int(r.get("flow_duration", 0) or 0),
            "packets": int(r.get("total_fwd_packets", 1) or 1),
            "bytes": int(r.get("total_len_fwd_pkts", 64) or 64),
            "is_attack": int(r.get("is_attack", 0)),
            "attack_category": r.get("attack_category", "Normal"),
            "attack_type": r.get("clean_label", "BENIGN")
        })

    packets_json_path = os.path.join(OUTPUTS_DIR, "streaming_packets.json")
    with open(packets_json_path, "w") as f:
        json.dump(export_packets, f, indent=2)
    print(f"[EXPORT] Exported {len(export_packets)} real-world streaming packets to: {packets_json_path}")

    # Stage micro-batch files for Spark Structured Streaming
    for b in range(num_batches):
        batch_slice = records[b * batch_size : (b + 1) * batch_size]
        if not batch_slice:
            continue
        batch_file = os.path.join(STREAM_INPUT_DIR, f"flow_batch_{b+1}_{int(time.time())}.json")
        with open(batch_file, "w") as f:
            for rec in batch_slice:
                f.write(json.dumps(rec) + "\n")
        print(f"  -> Generated streaming batch {b+1}/{num_batches} ({len(batch_slice)} flows) -> {os.path.basename(batch_file)}")

def run_structured_streaming_detection(spark=None, runtime_seconds=6):
    """
    Spark Structured Streaming query monitoring the real-world micro-batch flow stream.
    """
    if spark is None:
        spark = get_spark_session("CICIDS_Streaming_NIDS")

    reset_streaming_directories()
    simulate_real_world_streaming(spark, num_batches=3, batch_size=150)

    print("\n[STREAMING] Initializing Spark Structured Streaming Engine on Real-World Micro-Batches...")

    stream_schema = StructType([
        StructField("dst_port", IntegerType(), True),
        StructField("flow_duration", DoubleType(), True),
        StructField("total_fwd_packets", DoubleType(), True),
        StructField("total_len_fwd_pkts", DoubleType(), True),
        StructField("is_attack", IntegerType(), True),
        StructField("attack_category", StringType(), True),
        StructField("clean_label", StringType(), True)
    ])

    streaming_df = (
        spark.readStream
        .schema(stream_schema)
        .json(STREAM_INPUT_DIR)
    )

    threat_aggregates = (
        streaming_df
        .groupBy("attack_category", "dst_port")
        .agg(
            F.count("*").alias("flow_count"),
            F.sum("is_attack").alias("threats_flagged"),
            F.round(F.avg("total_len_fwd_pkts"), 1).alias("avg_bytes")
        )
    )

    query = (
        threat_aggregates.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", STREAM_CHECKPOINT_DIR)
        .start()
    )

    print(f"[STREAMING] Real-time engine monitoring '{STREAM_INPUT_DIR}' for {runtime_seconds} seconds...")
    query.awaitTermination(timeout=runtime_seconds)
    query.stop()
    print("[STREAMING] Real-world streaming simulation complete.\n")

if __name__ == "__main__":
    spark = get_spark_session("StreamingRunner")
    run_structured_streaming_detection(spark, runtime_seconds=6)
