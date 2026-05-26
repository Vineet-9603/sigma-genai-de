from typing import Dict, List, Union
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def detect_schema_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str]) -> Dict[str, Union[Dict[str, str], List, str]]:
    """
    Detects schema drift between expected and actual schemas.

    Args:
        expected_schema (Dict[str, str]): The expected schema.
        actual_schema (Dict[str, str]): The actual schema.

    Returns:
        Dict[str, Union[Dict[str, str], List, str]]: A report on the schema drift.
    """
    new_columns = {k: v for k, v in actual_schema.items() if k not in expected_schema}
    removed_columns = {k: v for k, v in expected_schema.items() if k not in actual_schema}
    type_changes = {k: (expected_schema[k], actual_schema[k]) for k in expected_schema if expected_schema[k]!= actual_schema.get(k, '')}
    drift_severity = 'NONE'
    if new_columns:
        if any(actual_schema[col] not in ['string', 'boolean'] or 'NULLABLE' not in actual_schema[col] for col in new_columns):
            drift_severity = 'HIGH'
        else:
            drift_severity = 'LOW'
    if removed_columns:
        drift_severity = 'BREAKING'
    return {
        "new_columns": new_columns,
        "removed_columns": list(removed_columns.keys()),
        "type_changes": type_changes,
        "drift_severity": drift_severity
    }

def decide_action(drift_report: Dict[str, Union[Dict[str, str], List, str]]) -> Dict[str, Dict[str, Union[str, str, str]]]:
    """
    Decides the action to take for each column based on the drift report.

    Args:
        drift_report (Dict[str, Union[Dict[str, str], List, str]]): The drift report.

    Returns:
        Dict[str, Dict[str, Union[str, str, str]]]: A decision for each column.
    """
    decisions = {}
    for col_name in drift_report["new_columns"]:
        if drift_report["new_columns"][col_name] == 'string':
            decisions[col_name] = {"action": "ADD_TO_SCHEMA", "reason": "New nullable string column", "risk_level": "LOW"}
        elif drift_report["new_columns"][col_name] in ['float', 'double']:
            decisions[col_name] = {"action": "FLAG_ANOMALY", "reason": "New float column", "risk_level": "HIGH"}
        else:
            decisions[col_name] = {"action": "ADD_TO_SCHEMA", "reason": "New nullable column", "risk_level": "LOW"}
    for col_name in drift_report["removed_columns"]:
        decisions[col_name] = {"action": "HALT", "reason": "Removed column", "risk_level": "BREAKING"}
    return decisions

def apply_schema_evolution(spark_df: DataFrame, decisions: Dict[str, Dict[str, Union[str, str, str]]], updated_schema: Dict[str, str]) -> Tuple[DataFrame, List[str]]:
    """
    Applies the schema evolution decisions to the DataFrame.

    Args:
        spark_df (DataFrame): The Spark DataFrame.
        decisions (Dict[str, Dict[str, Union[str, str, str]]]): The decisions to apply.
        updated_schema (Dict[str, str]): The updated schema.

    Returns:
        Tuple[DataFrame, List[str]]: The evolved DataFrame and a list of migration notes.
    """
    migration_notes = []
    for col_name, decision in decisions.items():
        if decision["action"] == "DROP_SILENTLY":
            spark_df = spark_df.drop(col_name)
        elif decision["action"] == "ADD_TO_SCHEMA":
            migration_notes.append(f"Added new column: {col_name}")
        elif decision["action"] == "FLAG_ANOMALY":
            spark_df = spark_df.withColumn(f"{col_name}_anomaly", F.when(F.col(col_name).isNull(), True).otherwise(False))
            migration_notes.append(f"Flagged anomaly for column: {col_name}")
        elif decision["action"] == "HALT":
            raise ValueError(f"Schema drift detected: {decision['reason']} for column: {col_name}")
    return spark_df, migration_notes

def handle_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str], spark_df: DataFrame = None) -> Dict[str, Union[Dict, List, Dict[str, Dict[str, Union[str, str, str]]]]]:
    """
    Handles the schema drift by detecting, deciding, and applying the necessary actions.

    Args:
        expected_schema (Dict[str, str]): The expected schema.
        actual_schema (Dict[str, str]): The actual schema.
        spark_df (DataFrame, optional): The Spark DataFrame. Defaults to None.

    Returns:
        Dict[str, Union[Dict, List, Dict[str, Dict[str, Union[str, str, str]]]]]: The full evolution report.
    """
    drift_report = detect_schema_drift(expected_schema, actual_schema)
    decisions = decide_action(drift_report)
    if spark_df is not None:
        evolved_df, migration_notes = apply_schema_evolution(spark_df, decisions, actual_schema)
        drift_report["migration_notes"] = migration_notes
    else:
        evolved_df = None
    print("Schema Drift Report:")
    print(drift_report)
    return {"drift_report": drift_report, "decisions": decisions, "evolved_df": evolved_df}
