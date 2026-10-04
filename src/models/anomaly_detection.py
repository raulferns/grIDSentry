import time
import numpy as np
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

def train_kmeans_anomaly_detector(train_df, test_df, k=6, seed=42):
    """
    Fits an unsupervised KMeans clustering model for anomaly detection.
    Computes distance to nearest cluster centroid; high distance indicates network anomaly / outlier.
    """
    print(f"\n[UNSUPERVISED] Training KMeans Anomaly Detector (k={k})...")
    t0 = time.time()
    kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=k, seed=seed, maxIter=20)
    model = kmeans.fit(train_df)
    train_time = time.time() - t0
    print(f"  -> KMeans trained in {train_time:.2f} seconds.")

    # Evaluate cluster separation via Silhouette with squared euclidean distance
    evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster", metricName="silhouette")
    train_predictions = model.transform(train_df)
    test_predictions = model.transform(test_df)

    silhouette_train = evaluator.evaluate(train_predictions)
    silhouette_test = evaluator.evaluate(test_predictions)
    print(f"  -> Silhouette Score (Train): {silhouette_train:.4f}")
    print(f"  -> Silhouette Score (Test):  {silhouette_test:.4f}")

    # Calculate distance to cluster center
    centers = model.clusterCenters()

    def calc_distance_to_center(features, cluster_id):
        if cluster_id is None or features is None:
            return 0.0
        center = centers[int(cluster_id)]
        feat_arr = features.toArray()
        diff = feat_arr - center
        return float(np.sqrt(np.dot(diff, diff)))

    dist_udf = F.udf(calc_distance_to_center, DoubleType())

    scored_test = test_predictions.withColumn("anomaly_score", dist_udf(F.col("features"), F.col("cluster")))

    # Compute 90th percentile threshold on sample to classify outlier as anomaly
    threshold = scored_test.approxQuantile("anomaly_score", [0.90], 0.01)[0]
    print(f"  -> Anomaly Distance Threshold (90th percentile): {threshold:.4f}")

    scored_test = scored_test.withColumn(
        "is_unsupervised_anomaly",
        F.when(F.col("anomaly_score") >= threshold, 1).otherwise(0)
    )

    return model, scored_test, {
        "train_time": round(train_time, 2),
        "silhouette_train": round(silhouette_train, 4),
        "silhouette_test": round(silhouette_test, 4),
        "anomaly_threshold": round(threshold, 4),
        "cluster_centers_count": len(centers)
    }
