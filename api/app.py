from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
from pathlib import Path

app = Flask(__name__)
CORS(app)

DATA_DIR = Path(__file__).parent.parent / 'data' / 'processed'

# ✅ เพิ่ม H12 ใน mapping
TIMEFRAME_FILES = {
    'D1': 'forex_combined.parquet',
    'H1': 'forex_combined_h1.parquet',
    'H12': 'forex_combined_h12.parquet',
}

@app.route('/api/pairs', methods=['GET'])
def get_pairs():
    try:
        df = pd.read_parquet(DATA_DIR / 'forex_combined.parquet')
        return jsonify(sorted(df['pair'].unique().tolist()))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/data/<pair>', methods=['GET'])
def get_pair_data(pair):
    try:
        timeframe = request.args.get('tf', 'D1').upper()
        limit = request.args.get('limit', 100, type=int)
        
        filename = TIMEFRAME_FILES.get(timeframe, 'forex_combined.parquet')
        file_path = DATA_DIR / filename
        
        if not file_path.exists():
            return jsonify({'error': f'No data for {timeframe}'}), 404
        
        df = pd.read_parquet(file_path)
        
        if 'timeframe' in df.columns:
            pair_df = df[(df['pair'] == pair.upper()) & (df['timeframe'] == timeframe)]
        else:
            pair_df = df[df['pair'] == pair.upper()]
        
        pair_df = pair_df.tail(limit)
        
        if 'date' in pair_df.columns and len(pair_df) > 0:
            pair_df['date'] = pair_df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        return jsonify(pair_df.to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/timeframes', methods=['GET'])
def get_timeframes():
    available = [tf for tf, f in TIMEFRAME_FILES.items() if (DATA_DIR / f).exists()]
    return jsonify(available)

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'ok', 'timeframes': available})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
