import os
import sys

# 1. Fix Windows TEMP/TMP if pointing to non-existent drive (e.g. D:\TEMP)
user_temp = os.path.expanduser(r"~\AppData\Local\Temp")
os.environ["TEMP"] = user_temp
os.environ["TMP"] = user_temp

# 2. Bind exact Python 3.12 interpreter for PySpark driver & workers (prevents Microsoft Store alias crash)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# 3. Configure local Hadoop winutils & native binaries
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
HADOOP_DIR = os.path.join(PROJECT_ROOT, "hadoop")
HADOOP_BIN = os.path.join(HADOOP_DIR, "bin")
if os.path.exists(HADOOP_BIN):
    os.environ["HADOOP_HOME"] = HADOOP_DIR
    os.environ["PATH"] = HADOOP_BIN + os.pathsep + os.environ.get("PATH", "")

# 4. Ensure JDK-17 is targeted explicitly for maximum stability with PySpark 3.5 / 4.x
JDK17_PATH = r"C:\Program Files\Java\jdk-17"
if os.path.exists(JDK17_PATH):
    os.environ["JAVA_HOME"] = JDK17_PATH
    os.environ["PATH"] = os.path.join(JDK17_PATH, "bin") + os.pathsep + os.environ.get("PATH", "")

# 5. JVM Reflection flags required by Java 17 for PySpark internal memory managers
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--driver-memory 4g "
    "--executor-memory 4g "
    '--driver-java-options "'
    '--add-opens=java.base/java.lang=ALL-UNNAMED '
    '--add-opens=java.base/java.lang.invoke=ALL-UNNAMED '
    '--add-opens=java.base/java.lang.reflect=ALL-UNNAMED '
    '--add-opens=java.base/java.io=ALL-UNNAMED '
    '--add-opens=java.base/java.net=ALL-UNNAMED '
    '--add-opens=java.base/java.nio=ALL-UNNAMED '
    '--add-opens=java.base/java.util=ALL-UNNAMED '
    '--add-opens=java.base/java.util.concurrent=ALL-UNNAMED '
    '--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED '
    '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED '
    '--add-opens=java.base/sun.nio.cs=ALL-UNNAMED '
    '--add-opens=java.base/sun.security.action=ALL-UNNAMED '
    '--add-opens=java.base/sun.util.calendar=ALL-UNNAMED '
    '--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED" '
    "pyspark-shell"
)

from pyspark.sql import SparkSession

_spark_instance = None

def get_spark_session(app_name="NetworkIntrusionDetection_BDA"):
    """
    Initializes or returns a singleton Apache SparkSession tuned for local Big Data analytics.
    """
    global _spark_instance
    if _spark_instance is not None:
        return _spark_instance

    warehouse_dir = os.path.abspath(os.path.join(PROJECT_ROOT, "data", "spark-warehouse"))

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.showConsoleProgress", "true")
        .config("spark.ui.enabled", "false")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    _spark_instance = spark
    return spark

if __name__ == "__main__":
    print("[INFO] Initializing SparkSession...")
    spark = get_spark_session("SparkSession_HealthCheck")
    print(f"[SUCCESS] SparkSession Active! Version: {spark.version}")
    df = spark.createDataFrame([(1, "Probe"), (2, "DoS"), (3, "Normal")], ["id", "attack_type"])
    df.show()
    print("[INFO] Healthcheck passed successfully!")
