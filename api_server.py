from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import pandas as pd
import json
import os
from datetime import datetime
import sys

# Import our history manager
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dividend_history_manager import DividendHistoryManager

app = Flask(__name__)
# Enable CORS for all origins (including file://)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Path to data files
DATA_DIR = 'output'

# Initialize history manager
history_manager = DividendHistoryManager()

@app.route('/api/dividends', methods=['GET'])
def get_dividends():
    """Get latest dividend data"""
    try:
        csv_path = os.path.join(DATA_DIR, 'etf_dividends.csv')
        
        if not os.path.exists(csv_path):
            response = jsonify({
                'error': 'No data available. Run the scraper first.',
                'data': [],
                'success': False
            })
            response.status_code = 404
            return response
        
        # Read CSV and convert to JSON
        df = pd.read_csv(csv_path)
        
        # Replace NaN values with 'N/A' before converting to dict
        df = df.fillna('N/A')
        
        # Convert to list of dictionaries
        data = df.to_dict('records')
        
        response = jsonify({
            'success': True,
            'data': data,
            'last_updated': df.iloc[0]['scraped_at'] if len(df) > 0 else None,
            'count': len(data)
        })
        response.headers['Content-Type'] = 'application/json'
        return response
    
    except Exception as e:
        response = jsonify({
            'error': str(e),
            'data': [],
            'success': False
        })
        response.status_code = 500
        return response

@app.route('/api/dividends/history', methods=['GET'])
def get_dividend_history():
    """Get full dividend history for all ETFs"""
    try:
        all_history = history_manager.get_all_history()
        
        # Add statistics for each ETF
        for ticker, data in all_history.items():
            stats = history_manager.get_statistics(ticker)
            data['statistics'] = stats
        
        return jsonify({
            'success': True,
            'data': all_history,
            'count': len(all_history)
        })
    
    except Exception as e:
        return jsonify({
            'error': str(e),
            'data': {},
            'success': False
        }), 500

@app.route('/api/dividends/history/<ticker>', methods=['GET'])
def get_ticker_history(ticker):
    """Get dividend history for a specific ticker"""
    try:
        history = history_manager.get_ticker_history(ticker.upper(), limit=52)
        stats = history_manager.get_statistics(ticker.upper())
        
        return jsonify({
            'success': True,
            'ticker': ticker.upper(),
            'history': history,
            'statistics': stats,
            'count': len(history)
        })
    
    except Exception as e:
        return jsonify({
            'error': str(e),
            'ticker': ticker.upper(),
            'history': [],
            'success': False
        }), 500

@app.route('/api/status', methods=['GET'])
def get_status():
    """Check if data is available and when it was last updated"""
    try:
        csv_path = os.path.join(DATA_DIR, 'etf_dividends.csv')
        
        if not os.path.exists(csv_path):
            return jsonify({
                'status': 'no_data',
                'message': 'No data available. Run the scraper first.'
            })
        
        # Get file modification time
        mod_time = os.path.getmtime(csv_path)
        mod_datetime = datetime.fromtimestamp(mod_time)
        
        # Read CSV to get scrape time
        df = pd.read_csv(csv_path)
        
        return jsonify({
            'status': 'ok',
            'file_modified': mod_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            'records': len(df),
            'etfs': df['ticker'].tolist()
        })
    
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/trigger-scrape', methods=['POST'])
def trigger_scrape():
    """Trigger a new scrape (runs the scraper)"""
    try:
        import subprocess
        
        # Run the scraper in the background
        result = subprocess.run(
            ['python', 'scraper.py'],
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode == 0:
            # Update history after successful scrape
            history_manager.update_from_csv()
            
            return jsonify({
                'success': True,
                'message': 'Scrape completed successfully and history updated',
                'output': result.stdout
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Scrape failed',
                'error': result.stderr
            }), 500
    
    except subprocess.TimeoutExpired:
        return jsonify({
            'success': False,
            'message': 'Scrape timed out (took longer than 5 minutes)'
        }), 500
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/')
def index():
    """Serve API documentation"""
    return jsonify({
        'name': 'ETF Dividend Scraper API',
        'version': '1.0',
        'endpoints': {
            '/api/dividends': 'GET - Get latest dividend data',
            '/api/dividends/history': 'GET - Get full dividend history',
            '/api/status': 'GET - Check data availability and freshness',
            '/api/trigger-scrape': 'POST - Manually trigger a new scrape',
            '/frontend': 'GET - Web interface'
        }
    })

@app.route('/frontend')
def frontend():
    """Serve the frontend HTML"""
    try:
        with open('frontend.html', 'r') as f:
            html_content = f.read()
        # Update API_URL in the HTML to use relative paths
        html_content = html_content.replace('http://localhost:5001', '')
        return html_content
    except FileNotFoundError:
        return "Frontend not found. Please ensure frontend.html is in the same directory.", 404

if __name__ == '__main__':
    # Create output directory if it doesn't exist
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Run the Flask app
    print("="*80)
    print("ETF Dividend API Server")
    print("="*80)
    print("Server running at: http://localhost:5000")
    print("API endpoints:")
    print("  - GET  http://localhost:5000/api/dividends")
    print("  - GET  http://localhost:5000/api/status")
    print("  - POST http://localhost:5000/api/trigger-scrape")
    print("="*80)
    
    app.run(host='0.0.0.0', port=5000, debug=True)