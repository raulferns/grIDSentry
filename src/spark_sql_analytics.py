import json
import os
from pyspark.sql import functions as F
from src.config import OUTPUTS_DIR
from src.spark_session import get_spark_session

def run_spark_sql_analytics(df, spark=None, export_json=True):
    """
    Registers a Spark SQL Temporary View on the 1.94 GB real-world dataset
    and executes deep distributed analytical queries.
    """
    if spark is None:
        spark = get_spark_session()

    print("\n" + "="*70)
    print("      REAL-WORLD SPARK SQL NETWORK THREAT INTELLIGENCE (1.94 GB)")
    print("="*70)

    df.createOrReplaceTempView("network_traffic")
    results = {}

    # Query 1: Macro Attack Category Breakdown
    print("\n[SPARK SQL] 1. Real-World Macro Attack Distribution & Percentages:")
    q1 = """
        SELECT
            attack_category,
            count(*) as count,
            round(count(*) * 100.0 / sum(count(*)) over(), 2) as percentage
        FROM network_traffic
        GROUP BY attack_category
        ORDER BY count DESC
    """
    df_q1 = spark.sql(q1)
    df_q1.show(truncate=False)
    results["attack_distribution"] = [r.asDict() for r in df_q1.collect()]

    # Query 2: Top 10 Attack Signatures
    print("\n[SPARK SQL] 2. Top Real-World Attack Signatures by Flow Frequency:")
    q2 = """
        SELECT
            clean_label as attack_type,
            attack_category,
            count(*) as frequency,
            round(avg(flow_duration) / 1000000.0, 2) as avg_duration_sec,
            round(avg(total_len_fwd_pkts), 1) as avg_fwd_bytes,
            round(avg(total_len_bwd_pkts), 1) as avg_bwd_bytes
        FROM network_traffic
        WHERE attack_category != 'Normal'
        GROUP BY clean_label, attack_category
        ORDER BY frequency DESC
        LIMIT 10
    """
    df_q2 = spark.sql(q2)
    df_q2.show(truncate=False)
    results["top_attacks"] = [r.asDict() for r in df_q2.collect()]

    # Query 3: Top Targeted Destination Ports via Window Functions
    print("\n[SPARK SQL] 3. Advanced Window Functions: Top 3 Targeted Ports per Attack Category:")
    q3 = """
        WITH RankedPorts AS (
            SELECT
                attack_category,
                dst_port,
                count(*) as service_traffic,
                ROW_NUMBER() OVER (PARTITION BY attack_category ORDER BY count(*) DESC) as rank
            FROM network_traffic
            WHERE attack_category != 'Normal' AND dst_port IS NOT NULL
            GROUP BY attack_category, dst_port
        )
        SELECT attack_category, dst_port as service, service_traffic, rank
        FROM RankedPorts
        WHERE rank <= 3
        ORDER BY attack_category, rank
    """
    df_q3 = spark.sql(q3)
    df_q3.show(truncate=False)
    results["ranked_services"] = [r.asDict() for r in df_q3.collect()]

    # Query 4: TCP Flag Threat Telemetry (SYN, RST, PSH, ACK flags)
    print("\n[SPARK SQL] 4. Threat Correlation with TCP Control Flags:")
    q4 = """
        SELECT
            attack_category,
            round(sum(syn_flag_cnt) * 1.0 / count(*), 4) as avg_syn_flags,
            round(sum(rst_flag_cnt) * 1.0 / count(*), 4) as avg_rst_flags,
            round(sum(psh_flag_cnt) * 1.0 / count(*), 4) as avg_psh_flags,
            round(sum(ack_flag_cnt) * 1.0 / count(*), 4) as avg_ack_flags,
            round(sum(fin_flag_cnt) * 1.0 / count(*), 4) as avg_fin_flags
        FROM network_traffic
        GROUP BY attack_category
        ORDER BY attack_category
    """
    df_q4 = spark.sql(q4)
    df_q4.show(truncate=False)
    results["flag_analysis"] = [r.asDict() for r in df_q4.collect()]

    # Query 5: Network Flow Rate Telemetry
    print("\n[SPARK SQL] 5. Flow Rate & Packet Size Telemetry per Threat Family:")
    q5 = """
        SELECT
            attack_category,
            round(avg(flow_bytes_s), 1) as avg_flow_bytes_sec,
            round(avg(flow_pkts_s), 1) as avg_flow_pkts_sec,
            round(avg(avg_pkt_size), 1) as avg_packet_size,
            round(avg(total_fwd_packets), 1) as avg_fwd_pkts
        FROM network_traffic
        GROUP BY attack_category
        ORDER BY attack_category
    """
    df_q5 = spark.sql(q5)
    df_q5.show(truncate=False)
    results["telemetry_signatures"] = [r.asDict() for r in df_q5.collect()]

    if export_json:
        out_path = os.path.join(OUTPUTS_DIR, "spark_sql_summary.json")
        with open(out_path, "w") as f:
            json.dump(results, f, indent=2, default=float)
        print(f"[EXPORT] Saved real-world Spark SQL analytics summary to: {out_path}")

    print("="*70 + "\n")
    return results
