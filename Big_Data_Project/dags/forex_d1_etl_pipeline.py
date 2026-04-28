"""
Forex D1 ETL Pipeline
=====================
ETL Pipeline for processing 9 Forex pairs daily data (D1 timeframe)
Updated for Airflow 3.x Compatibility
"""
# ========== ADD THIS AT THE VERY TOP ==========
import os
import sys

# ฝังค่าตัวแปรโดยตรงในไฟล์ (วิธีนี้ชัวร์กว่า export)
os.environ['AIRFLOW_HOME'] = '/home/theek/airflow'
os.environ['AIRFLOW__CORE__DAGS_FOLDER'] = '/home/theek/Downloads/Big_Data_Project/dags'
os.environ['AIRFLOW__CORE__LOAD_EXAMPLES'] = 'False'
os.environ['AIRFLOW__CORE__EXECUTOR'] = 'LocalExecutor'

# เพิ่ม path เพื่อให้ import หาไฟล์เจอ
sys.path.insert(0, '/home/theek/Downloads/Big_Data_Project')
# ========== END ADD ==========

from airflow import DAG
from airflow.operators.python import PythonOperator
# ... (โค้ดเดิมต่อท้าย)
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import glob
import logging
from pathlib import Path

# ==================== CONFIGURATION ====================
default_args = {
    'owner': 'forex_data_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

dag = DAG(
    'forex_d1_etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline for 9 Forex Pairs (Daily D1)',
    # ✅ แก้ไข: เปลี่ยนจาก schedule_interval เป็น schedule
    schedule='@daily', 
    # ✅ แก้ไข: ใช้ datetime แทน days_ago
    start_date=datetime(2026, 4, 28), 
    catchup=False,
    tags=['forex', 'etl', 'daily', 'market_data'],
    max_active_runs=1,
    doc_md="""
    **Pipeline นี้สำหรับประมวลผลข้อมูล Forex รายวัน (D1)**
    - Extract: อ่าน CSV
    - Validate: ตรวจสอบข้อมูล
    - Transform: สร้าง Features
    - Load: บันทึกผล
    """
)

# 📂 กำหนด Path แบบ Hardcode เพื่อให้แน่ใจว่าหาไฟล์เจอ (แก้ไขตาม user ของคุณ)
RAW_DIR = '/home/theek/Downloads/Big_Data_Project/data/raw'
PROCESSED_DIR = '/home/theek/Downloads/Big_Data_Project/data/processed'
os.makedirs(PROCESSED_DIR, exist_ok=True)

REQUIRED_COLS = ['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume', 'date']

# ==================== TASK FUNCTIONS ====================

def extract_data(**kwargs):
    """Task 1: Extract"""
    ti = kwargs['ti']
    files = sorted(glob.glob(os.path.join(RAW_DIR, '*_D1.csv')))
    
    if not files:
        raise FileNotFoundError(f"ไม่พบไฟล์ CSV ในโฟลเดอร์: {RAW_DIR}")
    
    file_metadata = [
        {
            'path': f,
            'pair': Path(f).stem.replace('_D1', ''),
            'size_bytes': os.path.getsize(f)
        }
        for f in files
    ]
    
    ti.xcom_push(key='file_list', value=file_metadata)
    logging.info(f"✅ พบไฟล์ทั้งหมด {len(files)} ไฟล์")
    return {'count': len(files)}


def validate_data(**kwargs):
    """Task 2: Validate"""
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
            
            missing_cols = [c for c in REQUIRED_COLS if c not in df.columns]
            
            price_errors = 0
            if not df.empty:
                price_errors = (
                    (df['high'] < df['low']).sum() +
                    (df['open'] <= 0).sum() +
                    (df['close'] <= 0).sum()
                )
            
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
    
    logging.info("✅ Validation Passed")
    return validation_report


def transform_data(**kwargs):
    """Task 3: Transform"""
    ti = kwargs['ti']
    file_list = ti.xcom_pull(key='file_list')
    
    processed_summary = []
    
    for file_info in file_list:
        filepath = file_info['path']
        pair = file_info['pair']
        
        df = pd.read_csv(filepath)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.sort_values('date').reset_index(drop=True)
        
        # Feature Engineering
        df['daily_return'] = df['close'].pct_change()
        df['daily_range'] = df['high'] - df['low']
        df['volatility_20d'] = df['daily_return'].rolling(20).std()
        df['ma_50'] = df['close'].rolling(50).mean()
        
        output_path = os.path.join(PROCESSED_DIR, f'{pair}_d1_processed.parquet')
        df.to_parquet(output_path, index=False, compression='snappy')
        
        processed_summary.append({'pair': pair, 'rows': len(df)})
        logging.info(f"✅ Transformed {pair} -> {output_path}")
        
    ti.xcom_push(key='processed_summary', value=processed_summary)
    return processed_summary


def load_to_warehouse(**kwargs):
    """Task 4: Load (ตัวอย่าง)"""
    ti = kwargs['ti']
    processed_summary = ti.xcom_pull(key='processed_summary')
    logging.info(f"✅ Load Task Completed for {len(processed_summary)} pairs")
    return {'loaded': len(processed_summary)}


# ==================== DAG DEFINITION ====================

t1 = PythonOperator(task_id='extract_raw_files', python_callable=extract_data, dag=dag)
t2 = PythonOperator(task_id='validate_schema_logic', python_callable=validate_data, dag=dag)
t3 = PythonOperator(task_id='transform_features', python_callable=transform_data, dag=dag)
t4 = PythonOperator(task_id='load_to_warehouse', python_callable=load_to_warehouse, dag=dag)

t1 >> t2 >> t3 >> t4