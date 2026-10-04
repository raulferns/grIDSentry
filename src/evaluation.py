import json
import os
import time
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator, MulticlassClassificationEvaluator
)
from src.config import OUTPUTS_DIR

def evaluate_classifier(predictions, model_name, is_multiclass=False):
    """
    Evaluates model predictions across Accuracy, Precision, Recall, F1, and AUC.
    Computes confusion matrix and inference latency.
    """
    print(f"\n" + "-"*50)
    print(f"  EVALUATING MODEL: {model_name}")
    print("-"*50)

    # Measure inference throughput
    t0 = time.time()
    total_records = predictions.count()
    infer_time = time.time() - t0
    throughput = (total_records / infer_time) if infer_time > 0 else 0

    # Evaluators
    acc_evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy"
    )
    f1_evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1"
    )
    prec_evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="weightedPrecision"
    )
    rec_evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="weightedRecall"
    )

    accuracy = acc_evaluator.evaluate(predictions)
    f1 = f1_evaluator.evaluate(predictions)
    precision = prec_evaluator.evaluate(predictions)
    recall = rec_evaluator.evaluate(predictions)

    roc_auc = None
    pr_auc = None
    if not is_multiclass and "probability" in predictions.columns:
        roc_eval = BinaryClassificationEvaluator(
            labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
        )
        pr_eval = BinaryClassificationEvaluator(
            labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR"
        )
        roc_auc = roc_eval.evaluate(predictions)
        pr_auc = pr_eval.evaluate(predictions)

    # Confusion matrix
    conf_matrix_df = (
        predictions.groupBy("label")
        .pivot("prediction")
        .count()
        .na.fill(0)
        .orderBy("label")
    )

    print(f"  Accuracy:            {accuracy * 100:.2f}%")
    print(f"  Precision (Wtd):     {precision * 100:.2f}%")
    print(f"  Recall (Wtd):        {recall * 100:.2f}%")
    print(f"  F1-Score:            {f1 * 100:.2f}%")
    if roc_auc is not None:
        print(f"  ROC-AUC:             {roc_auc:.4f}")
        print(f"  PR-AUC:              {pr_auc:.4f}")
    print(f"  Inference Latency:   {infer_time:.2f}s ({throughput:,.0f} records/sec)")
    print("\n  Confusion Matrix (Rows: True, Cols: Predicted):")
    conf_matrix_df.show()

    conf_matrix_data = [row.asDict() for row in conf_matrix_df.collect()]

    metrics = {
        "model_name": model_name,
        "accuracy": round(accuracy * 100, 2),
        "precision": round(precision * 100, 2),
        "recall": round(recall * 100, 2),
        "f1_score": round(f1 * 100, 2),
        "roc_auc": round(roc_auc, 4) if roc_auc is not None else None,
        "pr_auc": round(pr_auc, 4) if pr_auc is not None else None,
        "inference_records_per_sec": round(throughput, 0),
        "confusion_matrix": conf_matrix_data
    }
    return metrics

def compare_all_models(models_dict, test_df, train_times=None, is_multiclass=False, export_json=True):
    """
    Evaluates all trained models on test data, compares metrics, and exports comparison summary.
    """
    all_metrics = []
    print("\n" + "="*70)
    print("           DISTRIBUTED SPARK ML MODEL BENCHMARK RESULTS")
    print("="*70)

    for name, model in models_dict.items():
        preds = model.transform(test_df)
        m = evaluate_classifier(preds, name, is_multiclass=is_multiclass)
        if train_times and name in train_times:
            m["train_time_sec"] = round(train_times[name], 2)
        all_metrics.append(m)

    if export_json:
        out_path = os.path.join(OUTPUTS_DIR, "model_benchmark_results.json")
        with open(out_path, "w") as f:
            json.dump(all_metrics, f, indent=2, default=float)
        print(f"\n[EXPORT] Model comparison exported to {out_path}")

    return all_metrics
