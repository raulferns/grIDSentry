import os
import time
from pyspark.ml.classification import (
    LogisticRegression, RandomForestClassifier, GBTClassifier
)
from src.config import MODELS_DIR

def train_logistic_regression(train_df, features_col="features", label_col="label"):
    """
    Trains a distributed Logistic Regression model using Spark MLlib.
    """
    print("\n[MODEL] Training Logistic Regression...")
    lr = LogisticRegression(
        featuresCol=features_col,
        labelCol=label_col,
        maxIter=25,
        regParam=0.01,
        elasticNetParam=0.1
    )
    t0 = time.time()
    model = lr.fit(train_df)
    train_time = time.time() - t0
    print(f"  -> Logistic Regression trained in {train_time:.2f} seconds.")
    return model, train_time

def train_random_forest(train_df, features_col="features", label_col="label", num_trees=35, max_depth=10):
    """
    Trains a distributed Random Forest Classifier.
    """
    print(f"\n[MODEL] Training Random Forest (Trees: {num_trees}, MaxDepth: {max_depth})...")
    rf = RandomForestClassifier(
        featuresCol=features_col,
        labelCol=label_col,
        numTrees=num_trees,
        maxDepth=max_depth,
        seed=42
    )
    t0 = time.time()
    model = rf.fit(train_df)
    train_time = time.time() - t0
    print(f"  -> Random Forest trained in {train_time:.2f} seconds.")
    return model, train_time

def train_gbt_classifier(train_df, features_col="features", label_col="label", max_iter=25, max_depth=6):
    """
    Trains a Gradient-Boosted Trees Classifier (Binary Classification).
    """
    print(f"\n[MODEL] Training Gradient-Boosted Trees (MaxIter: {max_iter}, MaxDepth: {max_depth})...")
    gbt = GBTClassifier(
        featuresCol=features_col,
        labelCol=label_col,
        maxIter=max_iter,
        maxDepth=max_depth,
        seed=42
    )
    t0 = time.time()
    model = gbt.fit(train_df)
    train_time = time.time() - t0
    print(f"  -> GBT Classifier trained in {train_time:.2f} seconds.")
    return model, train_time

def train_all_models(train_df, is_multiclass=False):
    """
    Trains baseline and ensemble models, tracking training latency.
    Note: GBT in Spark ML currently supports binary classification.
    """
    models = {}
    train_times = {}

    # 1. Logistic Regression
    lr_model, lr_time = train_logistic_regression(train_df)
    models["Logistic Regression"] = lr_model
    train_times["Logistic Regression"] = lr_time

    # 2. Random Forest
    rf_model, rf_time = train_random_forest(train_df)
    models["Random Forest"] = rf_model
    train_times["Random Forest"] = rf_time

    # 3. GBT (Binary only)
    if not is_multiclass:
        gbt_model, gbt_time = train_gbt_classifier(train_df)
        models["Gradient Boosted Trees"] = gbt_model
        train_times["Gradient Boosted Trees"] = gbt_time

    return models, train_times
