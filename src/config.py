import os

# Base Directories
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")
CICIDS_RAW_DIR = os.path.join(DATA_DIR, "raw_cicids")
PARQUET_DIR = os.path.join(DATA_DIR, "parquet_cicids")
STREAMING_DIR = os.path.join(DATA_DIR, "streaming")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
MODELS_DIR = os.path.join(BASE_DIR, "models")

for d in [DATA_DIR, CICIDS_RAW_DIR, PARQUET_DIR, STREAMING_DIR, OUTPUTS_DIR, MODELS_DIR]:
    os.makedirs(d, exist_ok=True)

# Macro Attack Taxonomy Mapping for CICIDS2017
ATTACK_CATEGORIES = {
    "BENIGN": "Normal",
    "DDoS": "DoS/DDoS",
    "DoS Hulk": "DoS/DDoS",
    "DoS GoldenEye": "DoS/DDoS",
    "DoS slowloris": "DoS/DDoS",
    "DoS Slowhttptest": "DoS/DDoS",
    "Heartbleed": "DoS/DDoS",
    "PortScan": "PortScan/Probe",
    "FTP-Patator": "Brute Force",
    "SSH-Patator": "Brute Force",
    "Bot": "Botnet",
    "Infiltration": "Infiltration"
}

# Standardized Core Continuous Telemetry Features for Spark MLlib
FEATURE_COLS = [
    "flow_duration",
    "total_fwd_packets",
    "total_bwd_packets",
    "total_len_fwd_pkts",
    "total_len_bwd_pkts",
    "fwd_pkt_len_max",
    "fwd_pkt_len_min",
    "fwd_pkt_len_mean",
    "fwd_pkt_len_std",
    "bwd_pkt_len_max",
    "bwd_pkt_len_min",
    "bwd_pkt_len_mean",
    "bwd_pkt_len_std",
    "flow_bytes_s",
    "flow_pkts_s",
    "flow_iat_mean",
    "flow_iat_std",
    "flow_iat_max",
    "flow_iat_min",
    "fwd_iat_mean",
    "bwd_iat_mean",
    "fwd_psh_flags",
    "fwd_urg_flags",
    "fwd_header_len",
    "bwd_header_len",
    "fwd_pkts_s",
    "bwd_pkts_s",
    "pkt_len_min",
    "pkt_len_max",
    "pkt_len_mean",
    "pkt_len_std",
    "pkt_len_var",
    "fin_flag_cnt",
    "syn_flag_cnt",
    "rst_flag_cnt",
    "psh_flag_cnt",
    "ack_flag_cnt",
    "urg_flag_cnt",
    "down_up_ratio",
    "avg_pkt_size",
    "init_win_bytes_fwd",
    "init_win_bytes_bwd",
    "active_mean",
    "idle_mean"
]
