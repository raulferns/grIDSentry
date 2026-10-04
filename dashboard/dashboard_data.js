window.DASHBOARD_DATA = {
  "pipeline_metadata": {
    "spark_version": "4.2.0",
    "dataset_name": "CICIDS2017 Real-World Benchmark",
    "total_records_ingested": 3119345,
    "train_records": 45098,
    "test_records": 14826,
    "pipeline_runtime_sec": 78.92
  },
  "storage_benchmark": {
    "csv_size_mb": 1147.37,
    "parquet_size_mb": 378.52,
    "compression_savings_pct": 67.01
  },
  "sql_analytics": {
    "attack_distribution": [
      {
        "attack_category": "Normal",
        "count": 2273097,
        "percentage": 72.87
      },
      {
        "attack_category": "DoS/DDoS",
        "count": 380699,
        "percentage": 12.2
      },
      {
        "attack_category": "Other Attack",
        "count": 288602,
        "percentage": 9.25
      },
      {
        "attack_category": "PortScan/Probe",
        "count": 158930,
        "percentage": 5.09
      },
      {
        "attack_category": "Brute Force/Web",
        "count": 16015,
        "percentage": 0.51
      },
      {
        "attack_category": "Botnet",
        "count": 1966,
        "percentage": 0.06
      },
      {
        "attack_category": "Infiltration",
        "count": 36,
        "percentage": 0.0
      }
    ],
    "top_attacks": [
      {
        "attack_type": null,
        "attack_category": "Other Attack",
        "frequency": 288602,
        "avg_duration_sec": 0.0,
        "avg_fwd_bytes": 0.0,
        "avg_bwd_bytes": 0.0
      },
      {
        "attack_type": "DoS Hulk",
        "attack_category": "DoS/DDoS",
        "frequency": 231073,
        "avg_duration_sec": 57.08,
        "avg_fwd_bytes": 281.3,
        "avg_bwd_bytes": 7770.7
      },
      {
        "attack_type": "PortScan",
        "attack_category": "PortScan/Probe",
        "frequency": 158930,
        "avg_duration_sec": 0.08,
        "avg_fwd_bytes": 1.1,
        "avg_bwd_bytes": 12.2
      },
      {
        "attack_type": "DDoS",
        "attack_category": "DoS/DDoS",
        "frequency": 128027,
        "avg_duration_sec": 16.96,
        "avg_fwd_bytes": 31.9,
        "avg_bwd_bytes": 7373.6
      },
      {
        "attack_type": "DoS GoldenEye",
        "attack_category": "DoS/DDoS",
        "frequency": 10293,
        "avg_duration_sec": 23.13,
        "avg_fwd_bytes": 418.4,
        "avg_bwd_bytes": 6561.3
      },
      {
        "attack_type": "FTP-Patator",
        "attack_category": "Brute Force/Web",
        "frequency": 7938,
        "avg_duration_sec": 4.51,
        "avg_fwd_bytes": 60.0,
        "avg_bwd_bytes": 93.9
      },
      {
        "attack_type": "SSH-Patator",
        "attack_category": "Brute Force/Web",
        "frequency": 5897,
        "avg_duration_sec": 6.17,
        "avg_fwd_bytes": 1008.6,
        "avg_bwd_bytes": 1382.6
      },
      {
        "attack_type": "DoS slowloris",
        "attack_category": "DoS/DDoS",
        "frequency": 5796,
        "avg_duration_sec": 56.55,
        "avg_fwd_bytes": 810.7,
        "avg_bwd_bytes": 20.6
      },
      {
        "attack_type": "DoS Slowhttptest",
        "attack_category": "DoS/DDoS",
        "frequency": 5499,
        "avg_duration_sec": 57.72,
        "avg_fwd_bytes": 496.6,
        "avg_bwd_bytes": 118.4
      },
      {
        "attack_type": "Bot",
        "attack_category": "Botnet",
        "frequency": 1966,
        "avg_duration_sec": 0.35,
        "avg_fwd_bytes": 2645.4,
        "avg_bwd_bytes": 63.8
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
        "service_traffic": 7937,
        "rank": 1
      },
      {
        "attack_category": "Brute Force/Web",
        "service": 22,
        "service_traffic": 5897,
        "rank": 2
      },
      {
        "attack_category": "Brute Force/Web",
        "service": 80,
        "service_traffic": 2181,
        "rank": 3
      },
      {
        "attack_category": "DoS/DDoS",
        "service": 80,
        "service_traffic": 380685,
        "rank": 1
      },
      {
        "attack_category": "DoS/DDoS",
        "service": 444,
        "service_traffic": 11,
        "rank": 2
      },
      {
        "attack_category": "DoS/DDoS",
        "service": 64873,
        "service_traffic": 1,
        "rank": 3
      },
      {
        "attack_category": "Infiltration",
        "service": 444,
        "service_traffic": 36,
        "rank": 1
      },
      {
        "attack_category": "Other Attack",
        "service": 80,
        "service_traffic": 288602,
        "rank": 1
      },
      {
        "attack_category": "PortScan/Probe",
        "service": 80,
        "service_traffic": 373,
        "rank": 1
      },
      {
        "attack_category": "PortScan/Probe",
        "service": 21,
        "service_traffic": 244,
        "rank": 2
      },
      {
        "attack_category": "PortScan/Probe",
        "service": 22,
        "service_traffic": 243,
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
        "avg_syn_flags": 0.2473,
        "avg_rst_flags": 0.0,
        "avg_psh_flags": 0.5594,
        "avg_ack_flags": 0.4406,
        "avg_fin_flags": 0.0
      },
      {
        "attack_category": "DoS/DDoS",
        "avg_syn_flags": 0.0072,
        "avg_rst_flags": 0.0,
        "avg_psh_flags": 0.2318,
        "avg_ack_flags": 0.6145,
        "avg_fin_flags": 0.1549
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
        "avg_syn_flags": 0.0549,
        "avg_rst_flags": 0.0003,
        "avg_psh_flags": 0.258,
        "avg_ack_flags": 0.2869,
        "avg_fin_flags": 0.0181
      },
      {
        "attack_category": "Other Attack",
        "avg_syn_flags": 0.0,
        "avg_rst_flags": 0.0,
        "avg_psh_flags": 0.0,
        "avg_ack_flags": 0.0,
        "avg_fin_flags": 0.0
      },
      {
        "attack_category": "PortScan/Probe",
        "avg_syn_flags": 0.0,
        "avg_rst_flags": 0.0,
        "avg_psh_flags": 0.9996,
        "avg_ack_flags": 0.0004,
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
        "avg_flow_bytes_sec": 396296.6,
        "avg_flow_pkts_sec": 63530.6,
        "avg_packet_size": 26.0,
        "avg_fwd_pkts": 8.3
      },
      {
        "attack_category": "DoS/DDoS",
        "avg_flow_bytes_sec": 351628.7,
        "avg_flow_pkts_sec": 110768.3,
        "avg_packet_size": 683.3,
        "avg_fwd_pkts": 5.1
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
        "avg_flow_bytes_sec": 1778450.2,
        "avg_flow_pkts_sec": 64728.0,
        "avg_packet_size": 124.1,
        "avg_fwd_pkts": 10.7
      },
      {
        "attack_category": "Other Attack",
        "avg_flow_bytes_sec": 0.0,
        "avg_flow_pkts_sec": 0.0,
        "avg_packet_size": 0.0,
        "avg_fwd_pkts": 0.0
      },
      {
        "attack_category": "PortScan/Probe",
        "avg_flow_bytes_sec": 220185.1,
        "avg_flow_pkts_sec": 62640.9,
        "avg_packet_size": 4.6,
        "avg_fwd_pkts": 1.0
      }
    ]
  },
  "model_benchmarks": [
    {
      "model_name": "Logistic Regression",
      "accuracy": 89.9,
      "precision": 89.9,
      "recall": 89.9,
      "f1_score": 89.5,
      "roc_auc": 0.9693,
      "pr_auc": 0.9315,
      "inference_records_per_sec": 8543.0,
      "confusion_matrix": [
        {
          "label": 0.0,
          "0.0": 10439,
          "1.0": 322
        },
        {
          "label": 1.0,
          "0.0": 1176,
          "1.0": 2889
        }
      ],
      "train_time_sec": 7.27
    },
    {
      "model_name": "Random Forest",
      "accuracy": 98.97,
      "precision": 98.97,
      "recall": 98.97,
      "f1_score": 98.96,
      "roc_auc": 0.9993,
      "pr_auc": 0.9983,
      "inference_records_per_sec": 179438.0,
      "confusion_matrix": [
        {
          "label": 0.0,
          "0.0": 10729,
          "1.0": 32
        },
        {
          "label": 1.0,
          "0.0": 121,
          "1.0": 3944
        }
      ],
      "train_time_sec": 3.98
    },
    {
      "model_name": "Gradient Boosted Trees",
      "accuracy": 99.18,
      "precision": 99.18,
      "recall": 99.18,
      "f1_score": 99.18,
      "roc_auc": 0.9993,
      "pr_auc": 0.9984,
      "inference_records_per_sec": 236775.0,
      "confusion_matrix": [
        {
          "label": 0.0,
          "0.0": 10722,
          "1.0": 39
        },
        {
          "label": 1.0,
          "0.0": 82,
          "1.0": 3983
        }
      ],
      "train_time_sec": 14.17
    }
  ],
  "unsupervised_kmeans": {
    "train_time": 4.15,
    "silhouette_train": 0.4265,
    "silhouette_test": 0.4176,
    "anomaly_threshold": 6.2884,
    "cluster_centers_count": 6
  }
};