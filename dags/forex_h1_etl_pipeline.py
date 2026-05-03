"""
Forex H1 ETL Pipeline
=====================
ETL Pipeline for processing 9 Forex pairs hourly data (H1 timeframe)
"""

import os
import glob
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# ==================== CONFIGURATION ====================
BASE_DIR = Path("/home/theek/Downloads/Big_Data_Project")
RAW_DIR = BASE_DIR / "data" / "raw" / "H1"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

REQUIRED_COLS = [
    "time",
    "open",
    "high",
    "low",
    "close",
    "tick_volume",
    "spread",
    "real_volume",
    "date",
]

default_args = {
    "owner": "forex_data_team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "forex_h1_etl_pipeline",
    default_args=default_args,
    description="ETL Pipeline for 9 Forex Pairs (Hourly H1)",
    schedule="@hourly",
    catchup=False,
    tags=["forex", "etl", "hourly", "h1"],
)

# ==================== TASK FUNCTIONS ====================


def extract_h1_data(**kwargs):
    """Task 1: Extract - อ่านไฟล์ CSV H1 ทั้งหมด"""
    ti = kwargs["ti"]
    files = sorted(glob.glob(str(RAW_DIR / "*_H1.csv")))

    if not files:
        raise FileNotFoundError(f"ไม่พบไฟล์ CSV H1 ในโฟลเดอร์: {RAW_DIR}")

    file_metadata = [
        {
            "path": f,
            "pair": Path(f).stem.replace("_H1", ""),
            "size_bytes": os.path.getsize(f),
        }
        for f in files
    ]

    ti.xcom_push(key="file_list", value=file_metadata)
    logging.info(f"✅ พบไฟล์ H1 จำนวน {len(files)} ไฟล์")
    return {"count": len(files)}


def validate_h1_data(**kwargs):
    """Task 2: Validate - ตรวจสอบความถูกต้องของข้อมูล H1"""
    ti = kwargs["ti"]
    file_list = ti.xcom_pull(key="file_list")

    if not file_list:
        raise ValueError("ไม่พบข้อมูลไฟล์จาก Task ก่อนหน้า")

    validation_report = {}
    critical_errors = []

    for file_info in file_list:
        filepath = file_info["path"]
        pair = file_info["pair"]

        try:
            df = pd.read_csv(
                filepath,
                header=None,
                names=[
                    "time",
                    "open",
                    "high",
                    "low",
                    "close",
                    "tick_volume",
                    "spread",
                    "real_volume",
                    "date",
                ],
            )

            missing_cols = [c for c in REQUIRED_COLS if c not in df.columns]

            price_errors = 0
            if not df.empty:
                price_errors = (
                    (df["high"] < df["low"]).sum()
                    + (df["open"] <= 0).sum()
                    + (df["close"] <= 0).sum()
                )

            validation_report[pair] = {
                "total_rows": len(df),
                "missing_columns": missing_cols,
                "price_logic_errors": int(price_errors),
                "status": "PASS",
            }

            if missing_cols or (len(df) > 0 and price_errors > len(df) * 0.05):
                validation_report[pair]["status"] = "FAIL"
                critical_errors.append(f"{pair}: Validation Failed")

        except Exception as e:
            validation_report[pair] = {"status": "ERROR", "error": str(e)}
            critical_errors.append(f"{pair}: {str(e)}")

    ti.xcom_push(key="validation_report", value=validation_report)

    if critical_errors:
        raise ValueError(f"Validation Failed: {critical_errors}")

    logging.info("✅ H1 Validation Passed")
    return validation_report


def transform_h1_data(**kwargs):
    """Task 3: Transform - สร้าง Features และบันทึกเป็น Parquet"""
    ti = kwargs["ti"]
    file_list = ti.xcom_pull(key="file_list")

    if not file_list:
        raise ValueError("ไม่พบข้อมูลไฟล์จาก Task ก่อนหน้า")

    processed_summary = []

    for file_info in file_list:
        filepath = file_info["path"]
        pair = file_info["pair"]

        df = pd.read_csv(
            filepath,
            header=None,
            names=[
                "time",
                "open",
                "high",
                "low",
                "close",
                "tick_volume",
                "spread",
                "real_volume",
                "date",
            ],
        )

        # เพิ่มคอลัมน์ pair
        df["pair"] = pair

        # แปลงคอลัมน์ date
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.sort_values("date").reset_index(drop=True)

        # Feature Engineering สำหรับ H1
        df["hourly_return"] = df["close"].pct_change()
        df["hourly_range"] = df["high"] - df["low"]
        df["volatility_20h"] = df["hourly_return"].rolling(20).std()
        df["ma_20"] = df["close"].rolling(20).mean()
        df["ma_50"] = df["close"].rolling(50).mean()

        # บันทึกเป็น Parquet
        output_path = PROCESSED_DIR / f"{pair}_h1_processed.parquet"
        df.to_parquet(output_path, index=False, compression="snappy")

        processed_summary.append(
            {"pair": pair, "rows": len(df), "output": str(output_path)}
        )
        logging.info(f"✅ Transformed H1 {pair} -> {output_path}")

    ti.xcom_push(key="processed_summary", value=processed_summary)
    return processed_summary


def load_h1_warehouse(**kwargs):
    """Task 4: Load - บันทึกข้อมูลรวม"""
    ti = kwargs["ti"]
    processed_summary = ti.xcom_pull(key="processed_summary")

    if processed_summary:
        all_dfs = []
        for item in processed_summary:
            parquet_path = Path(item["output"])
            if parquet_path.exists():
                df = pd.read_parquet(parquet_path)
                all_dfs.append(df)

        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            combined_path = PROCESSED_DIR / "forex_combined_h1.parquet"
            combined_df.to_parquet(combined_path, index=False)
            logging.info(f"✅ Saved combined H1 data to {combined_path}")

    logging.info(f"✅ H1 Load Task Completed for {len(processed_summary)} pairs")
    return {"loaded": len(processed_summary)}


# ==================== DAG TASKS ====================
t1 = PythonOperator(
    task_id="extract_h1_files", python_callable=extract_h1_data, dag=dag
)
t2 = PythonOperator(
    task_id="validate_h1_schema_logic", python_callable=validate_h1_data, dag=dag
)
t3 = PythonOperator(
    task_id="transform_h1_features", python_callable=transform_h1_data, dag=dag
)
t4 = PythonOperator(
    task_id="load_h1_warehouse", python_callable=load_h1_warehouse, dag=dag
)

# กำหนดลำดับการทำงาน
t1 >> t2 >> t3 >> t4
