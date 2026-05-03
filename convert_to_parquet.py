import pandas as pd
from pathlib import Path
import glob

BASE_DIR = Path("/home/theek/Downloads/Big_Data_Project")
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

COLUMNS = [
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


def process_file(filepath, timeframe):
    """ประมวลผลไฟล์ CSV เดียว"""
    pair = Path(filepath).stem.replace(f"_{timeframe}", "")
    print(f"🔄 Processing {pair} ({timeframe})...")

    df = pd.read_csv(filepath, header=None, names=COLUMNS)
    df["pair"] = pair
    df["timeframe"] = timeframe
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values("date").reset_index(drop=True)

    return df


# ==================== ประมวลผลไฟล์ H1 ====================
print("\n" + "=" * 60)
print("📊 ประมวลผลไฟล์ H1 (Hourly)")
print("=" * 60)

h1_files = glob.glob(str(RAW_DIR / "H1" / "*_H1.csv"))
h1_dfs = []

for f in h1_files:
    try:
        df = process_file(f, "H1")
        h1_dfs.append(df)
        print(f"   ✅ {Path(f).name}: {len(df)} rows")
    except Exception as e:
        print(f"   ❌ Error: {e}")

if h1_dfs:
    h1_combined = pd.concat(h1_dfs, ignore_index=True)
    h1_path = PROCESSED_DIR / "forex_combined_h1.parquet"
    h1_combined.to_parquet(h1_path, index=False)
    print(f"\n✅ บันทึก H1: {h1_path}")
    print(f"📊 Total H1 rows: {len(h1_combined)}")

# ==================== ประมวลผลไฟล์ D1 ====================
print("\n" + "=" * 60)
print("📊 ประมวลผลไฟล์ D1 (Daily)")
print("=" * 60)

d1_files = glob.glob(str(RAW_DIR / "*_D1.csv"))
d1_dfs = []

for f in d1_files:
    try:
        df = process_file(f, "D1")
        d1_dfs.append(df)
        print(f"   ✅ {Path(f).name}: {len(df)} rows")
    except Exception as e:
        print(f"   ❌ Error: {e}")

if d1_dfs:
    d1_combined = pd.concat(d1_dfs, ignore_index=True)
    d1_path = PROCESSED_DIR / "forex_combined.parquet"
    d1_combined.to_parquet(d1_path, index=False)
    print(f"\n✅ บันทึก D1: {d1_path}")
    print(f"📊 Total D1 rows: {len(d1_combined)}")

print("\n🎉 เสร็จสิ้น! ไฟล์ Parquet พร้อมใช้งาน")
