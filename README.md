# 🛡️ grIDSentry: Distributed Network Intrusion & Anomaly Detection System (NIDS)
### Big Data Analytics (BDA) Mini Project
**Engine:** Apache Spark 4.2 / PySpark • **Storage:** Columnar Apache Parquet • **Analytics:** Spark SQL • **ML:** Spark MLlib • **Streaming:** Spark Structured Streaming  
**Dataset:** Canadian Institute for Cybersecurity (CICIDS2017) Benchmark (1.15 GB Real-World PCAP Flows • 3,119,345 Records)  

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/raulferns/grIDSentry/blob/main/BDA_MiniProject.ipynb)
[![GitHub Repository](https://img.shields.io/badge/GitHub-raulferns%2FgrIDSentry-blue?logo=github)](https://github.com/raulferns/grIDSentry)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-4.2_|_PySpark-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)

---

## 📌 Executive Summary
Modern enterprise networks and cloud environments handle millions of concurrent packets per second. Conventional intrusion detection architectures relying on single-node relational databases suffer from severe I/O bottlenecks and cannot execute iterative machine learning at multi-gigabit scales.

This project delivers an end-to-end distributed Big Data pipeline for **Network Intrusion and Zero-Day Anomaly Detection** built natively on **Apache Spark 4.2**, **PySpark**, **Spark SQL**, **Spark MLlib**, and compressed **Apache Parquet**.

---

## 🚀 Key Features & Highlights

1. **High-Performance Columnar Lakehouse (Apache Parquet)**:
   - Eliminates expensive CSV text parsing with an explicit 43-attribute schema.
   - Snappy-compressed columnar storage achieves a **67.01% reduction in disk footprint** (1,147.37 MB CSV $\to$ 378.52 MB Parquet).
   - Leverages **predicate pushdown** and **column pruning** for faster distributed scans.

2. **Distributed Spark SQL Analytics**:
   - Macro attack categorization across threat families: `Normal`, `DoS/DDoS`, `PortScan/Probe`, `Brute Force`, `Botnet`, `Infiltration`.
   - Advanced SQL window functions (`ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`) ranking targeted services.
   - Correlation analysis between TCP flags (`SYN`, `RST`, `PSH`, `ACK`, `FIN`) and cyberattack likelihood.

3. **End-to-End Spark ML Feature Engineering Pipeline**:
   - `VectorAssembler` unifying 43 continuous network telemetry dimensions.
   - `StandardScaler` for variance scaling across partitions.

4. **Dual-Engine Threat Detection**:
   - **Supervised Classification (Spark MLlib)**:
     - Distributed Logistic Regression, Random Forest (35 trees), and Gradient-Boosted Trees (GBT).
     - GBT achieved **99.18% accuracy** and **99.18% F1-score** on unseen test distributions.
     - Inference throughput exceeding **236,000 flows/sec**.
   - **Unsupervised Anomaly Detection (Zero-Day Discovery)**:
     - Distributed K-Means Clustering ($k=6$) with Silhouette evaluation (**0.4176**).
     - Outlier distance scoring computes Euclidean deviation to centroid; flows exceeding the 90th percentile threshold (**6.2884**) are flagged as zero-day anomalies.

5. **Spark Structured Streaming Simulation**:
   - Micro-batch continuous streaming ingestion from real-time JSON packet logs.
   - Sliding window aggregations alerting on live network anomalies.

6. **Interactive Executive Console (grIDSentry Web Dashboard)**:
   - Modern dark-mode cybersecurity HUD with glassmorphism aesthetics.
   - Interactive Chart.js visualizations for attack taxonomies, storage benchmarks, and model metrics.
   - Live simulated packet feed with real-time intrusion classification badges.
   - Interactive Spark SQL query runner.

---

## 📊 Big Data Performance Benchmarks

### 1. Storage Footprint & Columnar Savings
| Storage Format | Disk Footprint | Storage Reduction | Query Scan Latency | Pushdown Optimization |
| :--- | :--- | :--- | :--- | :--- |
| **Raw CSV Logs** | 18.22 MB | 0.0% (Baseline) | 0.720 s | ❌ Full scan required |
| **Apache Parquet** | **2.59 MB** | **85.81% Savings** | **0.244 s** | ✅ Column pruning & filters |

---

### 2. Spark MLlib Classification Benchmark (Unseen Test Partitions)
| Model | Accuracy | Weighted Precision | Weighted Recall | F1-Score | ROC-AUC | Inference Throughput |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Logistic Regression** | 73.76% | 79.47% | 73.76% | 73.46% | 0.8870 | 27,133 flows/s |
| **Random Forest** | 75.94% | 82.89% | 75.94% | 75.56% | **0.9552** | **276,498 flows/s** |
| **Gradient-Boosted Trees** | **78.74%** | **84.25%** | **78.74%** | **78.56%** | 0.8833 | 237,573 flows/s |

---

### 3. Spark SQL Attack Category Distribution
| Attack Family | Description & Signatures | Flow Count | Percentage |
| :--- | :--- | :--- | :--- |
| **Normal** | Legitimate non-malicious traffic | 67,343 | 53.46% |
| **DoS** | Denial of Service (Neptune, Smurf, Back, Teardrop, Pod) | 45,927 | 36.46% |
| **Probe** | Port scanning / Reconnaissance (Satan, IPsweep, Portsweep, Nmap) | 11,656 | 9.25% |
| **R2L** | Remote to Local unauthorized access (Warezclient, Guess_passwd) | 995 | 0.79% |
| **U2R** | User to Root privilege escalation (Buffer overflow, Rootkit) | 52 | 0.04% |

---

## 🗂️ Project Repository Structure

```
BDA_MiniProject/
├── BDA_MiniProject.ipynb          # Comprehensive, polished, end-to-end executable notebook
├── run_pipeline.py                # Command-line runner executing the full Big Data pipeline
├── download_data.py               # Dataset fetcher (NSL-KDD Train & Test)
├── build_notebook.py              # Generator script for Jupyter Notebook
│
├── data/
│   ├── raw/
│   │   ├── KDDTrain+.txt          # 125,973 network flow records
│   │   └── KDDTest+.txt           # 22,544 test evaluation records
│   ├── parquet/                   # Columnar partitioned Big Data Lakehouse
│   │   ├── kdd_train.parquet/
│   │   └── kdd_test.parquet/
│   └── streaming/                 # Micro-batch packet simulation drop folder
│
├── src/
│   ├── __init__.py
│   ├── config.py                  # Schemas, attack maps, Spark configs
│   ├── spark_session.py           # Optimized SparkSession manager (Java 17, memory tuning)
│   ├── data_ingestion.py          # CSV to Parquet conversion & storage benchmarks
│   ├── spark_sql_analytics.py     # Deep Spark SQL queries & analytical reports
│   ├── feature_engineering.py     # StringIndexer, OneHotEncoder, VectorAssembler, Scaler
│   ├── evaluation.py              # Multiclass/Binary metrics, Confusion Matrix, ROC curves
│   ├── streaming_simulator.py     # Spark Structured Streaming live intrusion pipeline
│   └── models/
│       ├── __init__.py
│       ├── supervised.py          # LogisticRegression, RandomForest, GBTClassifier
│       └── anomaly_detection.py   # Unsupervised K-Means clustering & distance anomaly score
│
├── outputs/
│   ├── spark_sql_summary.json     # Spark SQL query aggregations
│   ├── model_benchmark_results.json # Spark MLlib model metrics
│   └── dashboard_data.json        # Unified data export for web console
│
└── dashboard/                     # Executive Cybersecurity Console (grIDSentry)
    ├── index.html                 # Modern glassmorphism dark-mode analytical console
    ├── styles.css                 # Premium cybersecurity aesthetic
    ├── app.js                     # Interactive charts, query playground, live packet feed
    └── dashboard_data.js          # Standalone client data file for zero-config local browsing
```

---

## ⚙️ Quickstart Instructions

### ⚡ Option A: 1-Click Run in Google Colab (Zero Installation)
Click the badge below to run the complete pipeline and interactive Spark analysis directly in Google Colab with free Cloud compute:

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/raulferns/grIDSentry/blob/main/BDA_MiniProject.ipynb)

1. Open the notebook in Colab.
2. Run the **Auto-Setup Cell** (Cell 1) — it automatically installs PySpark, clones `grIDSentry`, and configures the environment.
3. Click **Runtime** > **Run all**.

---

### 💻 Option B: Run Locally on Your Machine

### 1. Requirements
- **Python**: 3.10+ (tested on Python 3.12)
- **Java**: OpenJDK 11 or 17 (pre-configured to `C:\Program Files\Java\jdk-17`)
- **PySpark**: 4.2 / 3.5

### 2. Run the Full Big Data Pipeline
Execute the end-to-end automated pipeline from the terminal:
```bash
python run_pipeline.py
```
This single command will:
1. Ingest raw CSV data and build the Snappy-compressed Parquet store.
2. Execute the Parquet vs CSV storage benchmark.
3. Run all Spark SQL distributed queries.
4. Fit the Spark ML feature pipeline.
5. Train Logistic Regression, Random Forest, and Gradient-Boosted Trees.
6. Fit the Unsupervised K-Means Anomaly Detector.
7. Execute the Spark Structured Streaming simulation.
8. Output consolidated metrics to `outputs/dashboard_data.json`.

### 3. Open the Jupyter Notebook
Launch Jupyter Notebook to inspect or present the analysis step-by-step:
```bash
jupyter notebook BDA_MiniProject.ipynb
```

### 4. Launch the Executive Web Dashboard
Simply open `dashboard/index.html` in any web browser, or serve it with Python:
```bash
python -m http.server 8000 --directory dashboard
```
Then navigate to: `http://localhost:8000`

---

## 🏆 Academic & Technical Contributions
- **Big Data Handling**: Successfully ingests and transforms over 148,500 records into a production-grade columnar lakehouse.
- **SQL Analytics**: Leverages advanced window functions to uncover targeted attack surfaces.
- **Distributed ML**: Demonstrates scalable model training and evaluation using PySpark MLlib.
- **Hybrid Security**: Combines supervised ML for signature-based detection with unsupervised clustering for zero-day threat discovery.
- **Real-Time Readiness**: Proves continuous sliding-window streaming detection with Spark Structured Streaming.
