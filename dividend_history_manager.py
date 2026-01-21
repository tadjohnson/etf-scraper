import pandas as pd
import json
from datetime import datetime
import os

class DividendHistoryManager:
    """Manages historical dividend data storage and retrieval"""
    
    def __init__(self, db_path='output/dividend_history.json'):
        self.db_path = db_path
        self.ensure_db_exists()
    
    def ensure_db_exists(self):
        """Create database file if it doesn't exist"""
        if not os.path.exists(self.db_path):
            initial_data = {
                'last_updated': datetime.now().isoformat(),
                'etfs': {}
            }
            self.save_db(initial_data)
    
    def load_db(self):
        """Load the dividend history database"""
        try:
            with open(self.db_path, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {
                'last_updated': datetime.now().isoformat(),
                'etfs': {}
            }
    
    def save_db(self, data):
        """Save the dividend history database"""
        data['last_updated'] = datetime.now().isoformat()
        with open(self.db_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    def add_dividend_record(self, ticker, amount, ex_date, pay_date, scraped_at):
        """Add a new dividend record for a ticker"""
        db = self.load_db()
        
        # Initialize ticker if not exists
        if ticker not in db['etfs']:
            db['etfs'][ticker] = {
                'name': '',
                'frequency': '',
                'dividends': []
            }
        
        # Parse amount (remove $ and convert to float)
        try:
            amount_float = float(amount.replace('$', '').strip()) if isinstance(amount, str) else float(amount)
        except (ValueError, AttributeError):
            amount_float = 0.0
        
        # Create new record
        new_record = {
            'amount': amount_float,
            'amount_str': amount if isinstance(amount, str) else f"${amount:.5f}",
            'ex_date': ex_date,
            'pay_date': pay_date,
            'scraped_at': scraped_at,
            'recorded_at': datetime.now().isoformat()
        }
        
        # Check if this dividend already exists (by ex_date)
        existing_dividends = db['etfs'][ticker]['dividends']
        
        # Remove any existing record with the same ex_date
        existing_dividends = [d for d in existing_dividends if d.get('ex_date') != ex_date]
        
        # Add new record
        existing_dividends.append(new_record)
        
        # Sort by ex_date (most recent first) and keep last 12 months
        existing_dividends.sort(key=lambda x: x.get('ex_date', ''), reverse=True)
        
        # Keep only last 52 records (for weekly) or 12 (for monthly) - we'll keep 52 to be safe
        db['etfs'][ticker]['dividends'] = existing_dividends[:52]
        
        self.save_db(db)
        return len(db['etfs'][ticker]['dividends'])
    
    def update_from_csv(self, csv_path='output/etf_dividends.csv'):
        """Update history from the current scraper CSV output"""
        if not os.path.exists(csv_path):
            print("CSV file not found")
            return
        
        df = pd.read_csv(csv_path)
        db = self.load_db()
        
        for _, row in df.iterrows():
            ticker = row['ticker']
            
            # Update ETF metadata
            if ticker not in db['etfs']:
                db['etfs'][ticker] = {
                    'name': row['name'],
                    'frequency': row['frequency'],
                    'dividends': []
                }
            else:
                db['etfs'][ticker]['name'] = row['name']
                db['etfs'][ticker]['frequency'] = row['frequency']
            
            # Add dividend record (only if we have valid data)
            if row['amount'] != 'N/A' and row['ex_date'] != 'N/A':
                self.add_dividend_record(
                    ticker=ticker,
                    amount=row['amount'],
                    ex_date=row['ex_date'],
                    pay_date=row['pay_date'],
                    scraped_at=row['scraped_at']
                )
        
        print(f"✓ Updated history for {len(df)} ETFs")
    
    def get_ticker_history(self, ticker, limit=12):
        """Get dividend history for a specific ticker"""
        db = self.load_db()
        
        if ticker not in db['etfs']:
            return []
        
        return db['etfs'][ticker]['dividends'][:limit]
    
    def get_all_history(self):
        """Get complete history for all tickers"""
        db = self.load_db()
        return db['etfs']
    
    def get_statistics(self, ticker):
        """Calculate statistics for a ticker's dividend history"""
        history = self.get_ticker_history(ticker, limit=52)
        
        if not history:
            return None
        
        amounts = [d['amount'] for d in history if d['amount'] > 0]
        
        if not amounts:
            return None
        
        return {
            'count': len(amounts),
            'latest': amounts[0] if amounts else 0,
            'average': sum(amounts) / len(amounts),
            'min': min(amounts),
            'max': max(amounts),
            'total_12m': sum(amounts[:12]) if len(amounts) >= 12 else sum(amounts),
            'trend': self._calculate_trend(amounts[:12])
        }
    
    def _calculate_trend(self, amounts):
        """Calculate simple trend (positive, negative, or stable)"""
        if len(amounts) < 2:
            return 'stable'
        
        # Compare recent average to older average
        recent_avg = sum(amounts[:3]) / min(3, len(amounts[:3]))
        older_avg = sum(amounts[3:6]) / min(3, len(amounts[3:6])) if len(amounts) > 3 else recent_avg
        
        diff_percent = ((recent_avg - older_avg) / older_avg * 100) if older_avg > 0 else 0
        
        if diff_percent > 5:
            return 'increasing'
        elif diff_percent < -5:
            return 'decreasing'
        else:
            return 'stable'


def main():
    """Test the history manager"""
    manager = DividendHistoryManager()
    
    # Update from current CSV
    manager.update_from_csv()
    
    # Display statistics
    print("\n" + "="*80)
    print("DIVIDEND HISTORY STATISTICS")
    print("="*80)
    
    all_history = manager.get_all_history()
    
    for ticker, data in all_history.items():
        stats = manager.get_statistics(ticker)
        
        print(f"\n{ticker} - {data['name']}")
        print(f"  Frequency: {data['frequency']}")
        print(f"  Records: {len(data['dividends'])}")
        
        if stats:
            print(f"  Latest: ${stats['latest']:.5f}")
            print(f"  Average: ${stats['average']:.5f}")
            print(f"  Range: ${stats['min']:.5f} - ${stats['max']:.5f}")
            print(f"  12-Month Total: ${stats['total_12m']:.2f}")
            print(f"  Trend: {stats['trend']}")
        
        # Show last 5 dividends
        print(f"  Recent dividends:")
        for i, div in enumerate(data['dividends'][:5], 1):
            print(f"    {i}. ${div['amount']:.5f} (Ex: {div['ex_date']})")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    main()