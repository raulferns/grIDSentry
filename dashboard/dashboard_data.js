window.DASHBOARD_DATA = {
  "pipeline_metadata": {
    "spark_version": "4.2.0",
    "dataset_name": "CICIDS2017 Real-World Benchmark",
    "total_records_ingested": 26182,
    "train_records": 19741,
    "test_records": 6441,
    "pipeline_runtime_sec": 93.56
  },
  "storage_benchmark": {
    "csv_size_mb": 12.05,
    "parquet_size_mb": 3.88,
    "compression_savings_pct": 67.8
  },
  "sql_analytics": {
    "attack_distribution": [
      {
        "attack_category": "Normal",
        "count": 12000,
        "percentage": 45.83
      },
      {
        "attack_category": "DoS/DDoS",
        "count": 5000,
        "percentage": 19.1
      },
      {
        "attack_category": "Brute Force/Web",
        "count": 4680,
        "percentage": 17.87
      },
      {
        "attack_category": "PortScan/Probe",
        "count": 2500,
        "percentage": 9.55
      },
      {
        "attack_category": "Botnet",
        "count": 1966,
        "percentage": 7.51
      },
      {
        "attack_category": "Infiltration",
        "count": 36,
        "percentage": 0.14
      }
    ],
    "top_attacks": [
      {
        "attack_type": "FTP-Patator",
        "attack_category": "Brute Force/Web",
        "frequency": 2500,
        "avg_duration_sec": 4.48,
        "avg_fwd_bytes": 59.9,
        "avg_bwd_bytes": 93.7
      },
      {
        "attack_type": "DoS slowloris",
        "attack_category": "DoS/DDoS",
        "frequency": 2500,
        "avg_duration_sec": 55.41,
        "avg_fwd_bytes": 774.6,
        "avg_bwd_bytes": 8.2
      },
      {
        "attack_type": "DDoS",
        "attack_category": "DoS/DDoS",
        "frequency": 2500,
        "avg_duration_sec": 15.08,
        "avg_fwd_bytes": 31.4,
        "avg_bwd_bytes": 8313.8
      },
      {
        "attack_type": "PortScan",
        "attack_category": "PortScan/Probe",
        "frequency": 2500,
        "avg_duration_sec": 0.06,
        "avg_fwd_bytes": 4.0,
        "avg_bwd_bytes": 151.3
      },
      {
        "attack_type": "Bot",
        "attack_category": "Botnet",
        "frequency": 1966,
        "avg_duration_sec": 0.35,
        "avg_fwd_bytes": 2645.4,
        "avg_bwd_bytes": 63.8
      },
      {
        "attack_type": "Web Attack - Brute Force",
        "attack_category": "Brute Force/Web",
        "frequency": 1507,
        "avg_duration_sec": 6.51,
        "avg_fwd_bytes": 2111.5,
        "avg_bwd_bytes": 3484.7
      },
      {
        "attack_type": "Web Attack - XSS",
        "attack_category": "Brute Force/Web",
        "frequency": 652,
        "avg_duration_sec": 6.72,
        "avg_fwd_bytes": 1197.6,
        "avg_bwd_bytes": 4519.0
      },
      {
        "attack_type": "Infiltration",
        "attack_category": "Infiltration",
        "frequency": 36,
        "avg_duration_sec": 78.41,
        "avg_fwd_bytes": 376725.8,
        "avg_bwd_bytes": 5013.7
      },
      {
        "attack_type": "Web Attack - Sql Injection",
        "attack_category": "Brute Force/Web",
        "frequency": 21,
        "avg_duration_sec": 2.87,
        "avg_fwd_bytes": 316.0,
        "avg_bwd_bytes": 1286.3
      }
    ],
    "ranked_services": [
      {
        "attack_category": "Botnet",
        "service": 8080,
        "service_traffic": 1261,
        "rank": 1
      },
      {
        "attack_category": "Botnet",
        "service": 1991,
        "service_traffic": 1,
        "rank": 2
      },
      {
        "attack_category": "Botnet",
        "service": 52040,
        "service_traffic": 1,
        "rank": 3
      },
      {
        "attack_category": "Brute Force/Web",
        "service": 21,
        "service_traffic": 2499,
        "rank": 1
      },
      {
        "attack_category": "Brute Force/Web",
        "service": 80,
        "service_traffic": 2181,
        "rank": 2
      },
      {
        "attack_category": "DoS/DDoS",
        "service": 80,
        "service_traffic": 5000,
        "rank": 1
      },
      {
        "attack_category": "Infiltration",
        "service": 444,
        "service_traffic": 36,
        "rank": 1
      },
      {
        "attack_category": "PortScan/Probe",
        "service": 80,
        "service_traffic": 121,
        "rank": 1
      },
      {
        "attack_category": "PortScan/Probe",
        "service": 443,
        "service_traffic": 83,
        "rank": 2
      },
      {
        "attack_category": "PortScan/Probe",
        "service": 21,
        "service_traffic": 66,
        "rank": 3
      }
    ],
    "flag_analysis": [
      {
        "attack_category": "Botnet",
        "avg_syn_flags": 0.0,
        "avg_rst_flags": 0.0,
        "avg_psh_flags": 0.6246,
        "avg_ack_flags": 0.3754,
        "avg_fin_flags": 0.0
      },
      {
        "attack_category": "Brute Force/Web",
        "avg_syn_flags": 0.2662,
        "avg_rst_flags": 0.0,
        "avg_psh_flags": 0.6951,
        "avg_ack_flags": 0.3049,
        "avg_fin_flags": 0.0
      },
      {
        "attack_category": "DoS/DDoS",
        "avg_syn_flags": 0.1148,
        "avg_rst_flags": 0.0,
        "avg_psh_flags": 0.6536,
        "avg_ack_flags": 0.3464,
        "avg_fin_flags": 0.0
      },
      {
        "attack_category": "Infiltration",
        "avg_syn_flags": 0.5556,
        "avg_rst_flags": 0.0,
        "avg_psh_flags": 0.1667,
        "avg_ack_flags": 0.8333,
        "avg_fin_flags": 0.0
      },
      {
        "attack_category": "Normal",
        "avg_syn_flags": 0.0477,
        "avg_rst_flags": 0.0003,
        "avg_psh_flags": 0.3407,
        "avg_ack_flags": 0.2398,
        "avg_fin_flags": 0.0083
      },
      {
        "attack_category": "PortScan/Probe",
        "avg_syn_flags": 0.0,
        "avg_rst_flags": 0.0,
        "avg_psh_flags": 0.99,
        "avg_ack_flags": 0.0096,
        "avg_fin_flags": 0.0
      }
    ],
    "telemetry_signatures": [
      {
        "attack_category": "Botnet",
        "avg_flow_bytes_sec": 305537.1,
        "avg_flow_pkts_sec": 46603.0,
        "avg_packet_size": 55.3,
        "avg_fwd_pkts": 3.2
      },
      {
        "attack_category": "Brute Force/Web",
        "avg_flow_bytes_sec": 322474.7,
        "avg_flow_pkts_sec": 48364.0,
        "avg_packet_size": 18.4,
        "avg_fwd_pkts": 8.0
      },
      {
        "attack_category": "DoS/DDoS",
        "avg_flow_bytes_sec": 45408.7,
        "avg_flow_pkts_sec": 1617.3,
        "avg_packet_size": 485.9,
        "avg_fwd_pkts": 5.3
      },
      {
        "attack_category": "Infiltration",
        "avg_flow_bytes_sec": 20188.2,
        "avg_flow_pkts_sec": 2820.1,
        "avg_packet_size": 166.3,
        "avg_fwd_pkts": 830.2
      },
      {
        "attack_category": "Normal",
        "avg_flow_bytes_sec": 2146862.8,
        "avg_flow_pkts_sec": 64723.0,
        "avg_packet_size": 168.5,
        "avg_fwd_pkts": 60.8
      },
      {
        "attack_category": "PortScan/Probe",
        "avg_flow_bytes_sec": 246108.2,
        "avg_flow_pkts_sec": 55281.0,
        "avg_packet_size": 19.8,
        "avg_fwd_pkts": 1.2
      }
    ]
  },
  "model_benchmarks": [
    {
      "model_name": "Logistic Regression",
      "accuracy": 84.85,
      "precision": 85.19,
      "recall": 84.85,
      "f1_score": 84.71,
      "roc_auc": 0.9468,
      "pr_auc": 0.9522,
      "inference_records_per_sec": 4934.0,
      "confusion_matrix": [
        {
          "label": 0.0,
          "0.0": 2262,
          "1.0": 677
        },
        {
          "label": 1.0,
          "0.0": 299,
          "1.0": 3203
        }
      ],
      "train_time_sec": 7.96
    },
    {
      "model_name": "Random Forest",
      "accuracy": 99.27,
      "precision": 99.27,
      "recall": 99.27,
      "f1_score": 99.27,
      "roc_auc": 0.999,
      "pr_auc": 0.9993,
      "inference_records_per_sec": 66672.0,
      "confusion_matrix": [
        {
          "label": 0.0,
          "0.0": 2915,
          "1.0": 24
        },
        {
          "label": 1.0,
          "0.0": 23,
          "1.0": 3479
        }
      ],
      "train_time_sec": 7.02
    },
    {
      "model_name": "Gradient Boosted Trees",
      "accuracy": 99.32,
      "precision": 99.32,
      "recall": 99.32,
      "f1_score": 99.32,
      "roc_auc": 0.9994,
      "pr_auc": 0.9995,
      "inference_records_per_sec": 78081.0,
      "confusion_matrix": [
        {
          "label": 0.0,
          "0.0": 2918,
          "1.0": 21
        },
        {
          "label": 1.0,
          "0.0": 23,
          "1.0": 3479
        }
      ],
      "train_time_sec": 15.02
    }
  ],
  "unsupervised_kmeans": {
    "train_time": 4.58,
    "silhouette_train": 0.2164,
    "silhouette_test": 0.2098,
    "anomaly_threshold": 5.8948,
    "cluster_centers_count": 6
  }
};