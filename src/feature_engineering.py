import os
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from src.config import FEATURE_COLS

def build_feature_pipeline(df, is_multiclass=False):
    """
    Constructs an end-to-end Spark ML Pipeline for real-world network flow telemetry.
    Selects valid numerical columns present in the input DataFrame.
    """
    stages = []

    # Filter features that exist in the dataframe
    valid_features = [c for c in FEATURE_COLS if c in df.columns]
    print(f"[PIPELINE] Assembling {len(valid_features)} validated continuous telemetry features...")

    # 1. Assemble continuous flow metrics into feature vector
    assembler = VectorAssembler(
        inputCols=valid_features,
        outputCol="raw_features",
        handleInvalid="skip"
    )
    stages.append(assembler)

    # 2. Standard Scaler to scale features without centering sparse structures
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=False
    )
    stages.append(scaler)

    if is_multiclass:
        label_indexer = StringIndexer(
            inputCol="attack_category",
            outputCol="label",
            handleInvalid="keep"
        )
        stages.append(label_indexer)

    pipeline = Pipeline(stages=stages)
    return pipeline

def sanitize_dataframe(df):
    """Guarantees 0.0 for any NaN, null, or Infinite values across all feature columns."""
    from pyspark.sql import functions as F
    for c in FEATURE_COLS:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.when(
                    F.isnan(F.col(c)) | F.col(c).isNull() | (F.col(c) == float('inf')) | (F.col(c) == float('-inf')),
                    0.0
                ).otherwise(F.col(c))
            )
    return df

def prepare_data_splits(train_df, test_df, is_multiclass=False):
    """
    Fits the feature engineering pipeline on training data and transforms both train and test sets.
    """
    print(f"[PIPELINE] Sanitizing features against NaN/Infinity values...")
    train_df = sanitize_dataframe(train_df)
    test_df = sanitize_dataframe(test_df)

    print(f"[PIPELINE] Fitting Spark ML Feature Transformation Pipeline on Real-World Flows...")
    pipeline = build_feature_pipeline(train_df, is_multiclass=is_multiclass)
    fitted_pipeline = pipeline.fit(train_df)

    train_transformed = fitted_pipeline.transform(train_df)
    test_transformed = fitted_pipeline.transform(test_df)

    if not is_multiclass:
        train_transformed = train_transformed.withColumn("label", train_transformed["is_attack"].cast("double"))
        test_transformed = test_transformed.withColumn("label", test_transformed["is_attack"].cast("double"))

    train_transformed = train_transformed.cache()
    test_transformed = test_transformed.cache()

    print("[PIPELINE] Feature transformation complete.")
    return train_transformed, test_transformed, fitted_pipeline
