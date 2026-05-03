"""
Forex H12 ETL Pipeline
=======================
ETL Pipeline for processing 9 Forex pairs 12-hour data (H12 timeframe)
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
BASE_DIR = Path('/home/theek/Downloads/Big_Data_Project')
RAW_DIR = BASE_DIR / 'data' / 'raw' / 'H12'
PROCESSED_DIR = BASE_DIR / 'data' / 'processed'
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

default_args = {
    'owner': 'forex_data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'forex_h12_etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline for 9 Forex Pairs (12-Hour H12)',
    schedule='@daily',
    catchup=False,
    tags=['forex', 'etl', '12h', 'h12'],
)


def extract_h12_data(**kwargs):
    """Extract - อ่านไฟล์ CSV H12 ทั้งหมด"""
    ti = kwargs['ti']
    files = sorted(glob.glob(str(RAW_DIR / '*_H12.csv')))
    
    if not files:
        raise FileNotFoundError(f"ไม่พบไฟล์ CSV H12 ในโฟลเดอร์: {RAW_DIR}")
    
    file_metadata = [
        {
            'path': f,
            'pair': Path(f).stem.replace('_H12', ''),
            'size_bytes': os.path.getsize(f)
        }
        for f in files
    ]
    
    ti.xcom_push(key='file_list', value=file_metadata)
    logging.info(f"✅ พบไฟล์ H12 จำนวน {len(files)} ไฟล์")
    return {'count': len(files)}


def validate_h12_data(**kwargs):
    """Validate - ตรวจสอบความถูกต้องของข้อมูล H12"""
    ti = kwargs['ti']
    file_list = ti.xcom_pull(key='file_list')
    
    if not file_list:
        raise ValueError("ไม่พบข้อมูลไฟล์จาก Task ก่อนหน้า")
    
    validation_report = {}
    critical_errors = []
    
    for file_info in file_list:
        filepath = file_info['path']
        pair = file_info['pair']
        
        try:
            df = pd.read_csv(filepath)
            
            # ตรวจสอบคอลัมน์ที่จำเป็น
            required_cols = ['time', 'open', 'high', 'low', 'close', 'date']
            missing_cols = [c for c in required_cols if c not in df.columns]
            
            # ตรวจสอบราคา
            if not df.empty:
                price_errors = (
                    (df['high'] < df['low']).sum() +
                    (df['open'] <= 0).sum() +
                    (df['close'] <= 0).sum()
                )
            else:
                price_errors = 0
            
            validation_report[pair] = {
                'total_rows': len(df),
                'missing_columns': missing_cols,
                'price_logic_errors': int(price_errors),
                'status': 'PASS'
            }
            
            if missing_cols or (len(df) > 0 and price_errors > len(df) * 0.05):
                validation_report[pair]['status'] = 'FAIL'
                critical_errors.append(f"{pair}: Validation Failed")
                
        except Exception as e:
            validation_report[pair] = {'status': 'ERROR', 'error': str(e)}
            critical_errors.append(f"{pair}: {str(e)}")
    
    ti.xcom_push(key='validation_report', value=validation_report)
    
    if critical_errors:
        raise ValueError(f"Validation Failed: {critical_errors}")
    
    logging.info("✅ H12 Validation Passed")
    return validation_report


def transform_h12_data(**kwargs):
    """Transform - สร้าง Features และบันทึกเป็น Parquet"""
    ti = kwargs['ti']
    file_list = ti.xcom_pull(key='file_list')
    
    if not file_list:
        raise ValueError("ไม่พบข้อมูลไฟล์จาก Task ก่อนหน้า")
    
    all_dfs = []
    processed_summary = []
    
    for file_info in file_list:
        filepath = file_info['path']
        pair = file_info['pair']
        
        df = pd.read_csv(filepath)
        
        # แปลงวันที่
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.sort_values('date').reset_index(drop=True)
        
        # เพิ่มคอลัมน์ pair และ timeframe
        df['pair'] = pair
        df['timeframe'] = 'H12'
        
        # Feature Engineering สำหรับ H12
        df['h12_return'] = df['close'].pct_change()
        df['h12_range'] = df['high'] - df['low']
        df['volatility_20'] = df['h12_return'].rolling(20).std()
        df['ma_20'] = df['close'].rolling(20).mean()
        df['ma_50'] = df['close'].rolling(50).mean()
        
        # แปลงคอลัมน์ตัวเลข
        numeric_cols = ['open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume', 'time']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # ตัดแถวที่มี date เป็น NaN
        df = df[df['date'].notna()]
        
        all_dfs.append(df)
        processed_summary.append({'pair': pair, 'rows': len(df)})
        logging.info(f"✅ Transformed H12 {pair}: {len(df)} rows")
    
    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        output_path = PROCESSED_DIR / 'forex_combined_h12.parquet'
        combined.to_parquet(output_path, index=False, compression='snappy')
        
        ti.xcom_push(key='processed_summary', value=processed_summary)
        logging.info(f"✅ Saved H12 combined: {len(combined)} rows, {output_path}")
    
    return {'rows': sum(item['rows'] for item in processed_summary) if processed_summary else 0}


def load_h12_warehouse(**kwargs):
    """Load - สรุปผลการโหลดข้อมูล"""
    ti = kwargs['ti']
    processed_summary = ti.xcom_pull(key='processed_summary')
    
    if not processed_summary:
        raise ValueError("ไม่พบข้อมูลที่ประมวลผล")
    
    # ตรวจสอบไฟล์ที่สร้าง
    output_path = PROCESSED_DIR / 'forex_combined_h12.parquet'
    if output_path.exists():
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        logging.info(f"📦 H12 Parquet size: {file_size_mb:.2f} MB")
    
    logging.info(f"✅ H12 Load Task Completed for {len(processed_summary)} pairs")
    return {'loaded': len(processed_summary), 'total_rows': sum(item['rows'] for item in processed_summary)}


# ==================== DAG TASKS ====================
t1 = PythonOperator(task_id='extract_h12_files', python_callable=extract_h12_data, dag=dag)
t2 = PythonOperator(task_id='validate_h12_schema_logic', python_callable=validate_h12_data, dag=dag)
t3 = PythonOperator(task_id='transform_h12_features', python_callable=transform_h12_data, dag=dag)
t4 = PythonOperator(task_id='load_h12_warehouse', python_callable=load_h12_warehouse, dag=dag)

# กำหนดลำดับการทำงาน
t1 >> t2 >> t3 >> t4

print("✅ forex_h12_etl_pipeline.py created")
