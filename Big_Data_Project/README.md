# 📊 Forex D1 ETL Pipeline

Pipeline สำหรับประมวลผลข้อมูลราคาตลาด Forex แบบรายวัน (Daily/D1) จำนวน 9 คู่สกุลเงิน ด้วย Apache Airflow

## 🎯 วัตถุประสงค์
- ✅ Extract: อ่านไฟล์ CSV จากแหล่งข้อมูล
- ✅ Validate: ตรวจสอบคุณภาพข้อมูล (Schema, Null, Logic)
- ✅ Transform: สร้าง Features สำหรับ Analytics/ML
- ✅ Load: บันทึกข้อมูลลง PostgreSQL Warehouse
- ✅ Monitor: แจ้งเตือนสถานะผ่าน Slack/Email

## 📦 คู่สกุลเงินที่รองรับ
| Pair | Description |
|------|-------------|
| `AUDCAD` | Australian Dollar / Canadian Dollar |
| `AUDCHF` | Australian Dollar / Swiss Franc |
| `AUDUSD` | Australian Dollar / US Dollar |
| `EURCAD` | Euro / Canadian Dollar |
| `EURUSD` | Euro / US Dollar |
| `GBPUSD` | British Pound / US Dollar |
| `NZDCAD` | New Zealand Dollar / Canadian Dollar |
| `USDCHF` | US Dollar / Swiss Franc |
| `USDJPY` | US Dollar / Japanese Yen |

## 🚀 การติดตั้งและรันแบบ Local

### 1. โคลนและเตรียมโปรเจกต์
```bash
git clone <your-repo>
cd forex-pipeline
mkdir -p data/raw data/processed logs plugins .airflow
