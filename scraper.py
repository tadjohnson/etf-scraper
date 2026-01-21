from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
from datetime import datetime
import json
from typing import Dict, List
import time
import re
import os

class ETFDividendScraper:
    def __init__(self):
        self.etfs = [
            {
                'ticker': 'YBTC',
                'name': 'Roundhill Bitcoin Covered Call Strategy ETF',
                'frequency': 'Weekly',
                'declare_day': 'Tuesday'
            },
            {
                'ticker': 'BTCI',
                'name': 'Neos Bitcoin Covered Call ETF',
                'frequency': 'Monthly',
                'declare_day': '3rd Tuesday'
            },
            {
                'ticker': 'QQQI',
                'name': 'Neos Nasdaq 100 High Income ETF',
                'frequency': 'Monthly',
                'declare_day': '3rd Tuesday'
            },
            {
                'ticker': 'IWMI',
                'name': 'Neos Russell 2000 High Income ETF',
                'frequency': 'Monthly',
                'declare_day': '3rd Tuesday'
            }
        ]
        
        self.driver = None
    
    def setup_driver(self):
        """Initialize Selenium WebDriver with Chromium"""
        print("Setting up Chromium WebDriver...")
        
        chrome_options = Options()
        chrome_options.add_argument('--headless=new')  # Run in background
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Use system Chromium binary
        chrome_options.binary_location = '/usr/bin/chromium'
        
        # Use system ChromeDriver
        from selenium.webdriver.chrome.service import Service
        service = Service(executable_path='/usr/bin/chromedriver')
        
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        print("✓ WebDriver ready")
    
    def close_driver(self):
        """Close the WebDriver"""
        if self.driver:
            self.driver.quit()
            print("\n✓ WebDriver closed")
    
    def scrape_stockanalysis(self, ticker: str) -> Dict:
        """Scrape stockanalysis.com for dividend data - gets historical"""
        url = f"https://stockanalysis.com/etf/{ticker.lower()}/dividend/"
        result = {
            'amount': None,
            'ex_date': None,
            'pay_date': None,
            'historical': []
        }
        
        try:
            print(f"    Trying stockanalysis.com for historical data...")
            self.driver.get(url)
            
            # Wait for the dividend table to load
            wait = WebDriverWait(self.driver, 10)
            
            # Look for table with dividend data
            try:
                table = wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                
                # Get all rows of data (not just first)
                rows = table.find_elements(By.TAG_NAME, "tr")
                
                for i, row in enumerate(rows[1:], 1):  # Skip header row
                    if i > 15:  # Get up to 15 recent dividends
                        break
                    
                    cells = row.find_elements(By.TAG_NAME, "td")
                    
                    if len(cells) >= 2:
                        ex_date = cells[0].text.strip()
                        
                        amount_text = cells[1].text.strip()
                        amount_match = re.search(r'\$?\s*([\d.]+)', amount_text)
                        
                        if amount_match:
                            amount = f"${amount_match.group(1)}"
                            pay_date = cells[2].text.strip() if len(cells) >= 3 else 'N/A'
                            
                            # Add to historical data
                            result['historical'].append({
                                'amount': amount,
                                'ex_date': ex_date,
                                'pay_date': pay_date
                            })
                            
                            # First one is the latest
                            if i == 1:
                                result['amount'] = amount
                                result['ex_date'] = ex_date
                                result['pay_date'] = pay_date
                
                if result['amount']:
                    print(f"      Found: {result['amount']} (Ex: {result['ex_date']}) + {len(result['historical'])-1} historical")
            
            except TimeoutException:
                print(f"      Table not found or timeout")
        
        except Exception as e:
            print(f"      Error: {e}")
        
        return result
    
    def scrape_dividendcom(self, ticker: str) -> Dict:
        """Scrape dividend.com for dividend data"""
        url = f"https://www.dividend.com/dividend-stocks/technology/semiconductors/{ticker.lower()}-{ticker.lower()}/"
        # Try generic ETF URL
        url = f"https://www.dividend.com/etfs/{ticker.lower()}/"
        
        result = {
            'amount': None,
            'ex_date': None,
            'pay_date': None,
        }
        
        try:
            print(f"    Trying dividend.com...")
            self.driver.get(url)
            time.sleep(3)  # Allow page to load
            
            # Look for dividend amount and dates in the page
            page_text = self.driver.find_element(By.TAG_NAME, "body").text
            
            # Try to find latest dividend amount
            amount_patterns = [
                r'Most Recent Dividend.*?\$\s*([\d.]+)',
                r'Dividend.*?\$\s*([\d.]+)\s*per share',
                r'Last Dividend.*?\$\s*([\d.]+)',
            ]
            
            for pattern in amount_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE | re.DOTALL)
                if match:
                    result['amount'] = f"${match.group(1)}"
                    break
            
            # Try to find ex-dividend date
            ex_date_match = re.search(r'Ex-Dividend Date[:\s]+([\d/]+)', page_text, re.IGNORECASE)
            if ex_date_match:
                result['ex_date'] = ex_date_match.group(1)
            
            # Try to find payment date
            pay_date_match = re.search(r'Payment Date[:\s]+([\d/]+)', page_text, re.IGNORECASE)
            if pay_date_match:
                result['pay_date'] = pay_date_match.group(1)
            
            if result['amount']:
                print(f"      Found: {result['amount']}")
        
        except Exception as e:
            print(f"      Error: {e}")
        
        return result
    
    def scrape_nasdaq(self, ticker: str) -> Dict:
        """Scrape Nasdaq for dividend data - gets historical data"""
        url = f"https://www.nasdaq.com/market-activity/etf/{ticker.lower()}/dividend-history"
        result = {
            'amount': None,
            'ex_date': None,
            'pay_date': None,
            'historical': []  # Store multiple dividends
        }
        
        try:
            print(f"    Trying nasdaq.com for historical data...")
            self.driver.get(url)
            
            # Wait for content to load
            time.sleep(5)
            
            # Look for dividend table
            try:
                wait = WebDriverWait(self.driver, 10)
                table = wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                
                rows = table.find_elements(By.TAG_NAME, "tr")
                
                # Process all rows (not just the first one)
                for i, row in enumerate(rows[1:], 1):  # Skip header
                    if i > 15:  # Get up to 15 recent dividends
                        break
                    
                    cells = row.find_elements(By.TAG_NAME, "td")
                    
                    if len(cells) >= 4:
                        ex_date = cells[0].text.strip()
                        amount_text = cells[2].text.strip()
                        
                        amount_match = re.search(r'\$?\s*([\d.]+)', amount_text)
                        if amount_match:
                            amount = f"${amount_match.group(1)}"
                            
                            pay_date = cells[5].text.strip() if len(cells) >= 6 else 'N/A'
                            
                            # Add to historical data
                            result['historical'].append({
                                'amount': amount,
                                'ex_date': ex_date,
                                'pay_date': pay_date
                            })
                            
                            # First one is the latest
                            if i == 1:
                                result['amount'] = amount
                                result['ex_date'] = ex_date
                                result['pay_date'] = pay_date
                
                if result['amount']:
                    print(f"      Found: {result['amount']} (Ex: {result['ex_date']}) + {len(result['historical'])-1} historical")
            
            except TimeoutException:
                print(f"      Table not found")
        
        except Exception as e:
            print(f"      Error: {e}")
        
        return result
    
    def get_dividend_data(self, ticker: str) -> Dict:
        """Try multiple sources to get dividend data including historical"""
        print(f"  Fetching dividend data for {ticker}...")
        
        result = {
            'amount': None,
            'ex_date': None,
            'pay_date': None,
            'historical': []
        }
        
        # Try different sources in order of reliability
        sources = [
            self.scrape_stockanalysis,
            self.scrape_nasdaq,
            self.scrape_dividendcom,
        ]
        
        for source in sources:
            try:
                data = source(ticker)
                
                # Update result with any non-null values found
                if data.get('amount') and not result['amount']:
                    result['amount'] = data['amount']
                if data.get('ex_date') and not result['ex_date']:
                    result['ex_date'] = data['ex_date']
                if data.get('pay_date') and not result['pay_date']:
                    result['pay_date'] = data['pay_date']
                
                # Collect historical data
                if data.get('historical') and len(data['historical']) > 0:
                    result['historical'] = data['historical']
                
                # If we have amount, ex-date, and historical data, we're good
                if result['amount'] and result['ex_date'] and len(result['historical']) > 0:
                    print(f"  ✓ Complete data found with {len(result['historical'])} historical records")
                    break
                
                time.sleep(2)  # Be polite between sources
            
            except Exception as e:
                print(f"    Source failed: {e}")
                continue
        
        if not result['amount']:
            print(f"  ✗ No data found from any source")
        
        return result
    
    def scrape_all(self) -> List[Dict]:
        """Scrape dividend data for all ETFs"""
        results = []
        all_historical = []
        
        print("="*80)
        print("ETF DIVIDEND SCRAPER - Using Selenium WebDriver")
        print("Fetching current AND historical dividend data (up to 15 records per ETF)")
        print("="*80)
        
        self.setup_driver()
        
        try:
            for etf in self.etfs:
                print(f"\nProcessing {etf['ticker']} ({etf['name']})...")
                
                dividend_data = self.get_dividend_data(etf['ticker'])
                
                # Add latest to results
                results.append({
                    'ticker': etf['ticker'],
                    'name': etf['name'],
                    'frequency': etf['frequency'],
                    'declare_day': etf['declare_day'],
                    'amount': dividend_data.get('amount') or 'N/A',
                    'ex_date': dividend_data.get('ex_date') or 'N/A',
                    'pay_date': dividend_data.get('pay_date') or 'N/A',
                    'record_date': 'N/A',
                    'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'status': 'Success' if (dividend_data.get('amount') and dividend_data.get('ex_date')) else 'Partial/No data'
                })
                
                # Collect all historical data
                if dividend_data.get('historical'):
                    for hist in dividend_data['historical']:
                        all_historical.append({
                            'ticker': etf['ticker'],
                            'name': etf['name'],
                            'amount': hist['amount'],
                            'ex_date': hist['ex_date'],
                            'pay_date': hist.get('pay_date', 'N/A'),
                            'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        })
                
                time.sleep(3)  # Be polite between ETFs
        
        finally:
            self.close_driver()
        
        # Save historical data to separate file
        if all_historical:
            self.save_historical_data(all_historical)
        
        return results
    
    def save_historical_data(self, historical_data: List[Dict]):
        """Save historical dividend data to CSV"""
        try:
            df = pd.DataFrame(historical_data)
            filename = 'output/etf_dividend_history_raw.csv'
            df.to_csv(filename, index=False)
            print(f"\n✓ Saved {len(historical_data)} historical dividend records to {filename}")
        except Exception as e:
            print(f"\n✗ Error saving historical data: {e}")
    
    def create_dataframe(self, results: List[Dict]) -> pd.DataFrame:
        """Convert results to pandas DataFrame"""
        df = pd.DataFrame(results)
        column_order = ['ticker', 'name', 'frequency', 'declare_day', 
                       'amount', 'ex_date', 'pay_date', 'record_date', 'status', 'scraped_at']
        df = df[column_order]
        return df
    
    def save_to_csv(self, df: pd.DataFrame, filename: str = 'output/etf_dividends.csv'):
        """Save results to CSV"""
        try:
            df.to_csv(filename, index=False)
            print(f"\n✓ Results saved to {filename}")
        except Exception as e:
            print(f"\n✗ Error saving CSV: {e}")
    
    def save_to_json(self, results: List[Dict], filename: str = 'output/etf_dividends.json'):
        """Save results to JSON"""
        try:
            with open(filename, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"✓ Results saved to {filename}")
        except Exception as e:
            print(f"✗ Error saving JSON: {e}")
    
    def print_summary(self, df: pd.DataFrame):
        """Print a formatted summary"""
        print("\n" + "="*100)
        print("ETF DIVIDEND DECLARATIONS SUMMARY")
        print("="*100)
        
        for _, row in df.iterrows():
            print(f"\n{row['ticker']} - {row['name']}")
            print(f"  Frequency: {row['frequency']} (declares on {row['declare_day']})")
            print(f"  Latest Dividend: {row['amount']}")
            print(f"  Ex-Date: {row['ex_date']}")
            print(f"  Pay Date: {row['pay_date']}")
            print(f"  Status: {row['status']}")
        
        print("\n" + "="*100)
        print(f"Scrape completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*100)
        
        success_count = len(df[df['status'] == 'Success'])
        total_count = len(df)
        print(f"\nSuccess Rate: {success_count}/{total_count} ETFs")


def main():
    """Main execution function"""
    scraper = ETFDividendScraper()
    
    try:
        # Scrape all ETFs (including historical)
        results = scraper.scrape_all()
        
        # Create DataFrame
        df = scraper.create_dataframe(results)
        
        # Display results
        scraper.print_summary(df)
        
        # Save results
        scraper.save_to_csv(df)
        scraper.save_to_json(results)
        
        # Import historical data into history manager
        print("\n" + "="*80)
        print("Importing historical data into database...")
        print("="*80)
        
        try:
            from dividend_history_manager import DividendHistoryManager
            
            history_manager = DividendHistoryManager()
            
            # Import from the raw historical data
            raw_history_file = 'output/etf_dividend_history_raw.csv'
            if os.path.exists(raw_history_file):
                df_history = pd.read_csv(raw_history_file)
                
                for _, row in df_history.iterrows():
                    history_manager.add_dividend_record(
                        ticker=row['ticker'],
                        amount=row['amount'],
                        ex_date=row['ex_date'],
                        pay_date=row['pay_date'],
                        scraped_at=row['scraped_at']
                    )
                
                print(f"✓ Imported {len(df_history)} historical records into database")
            
            # Also update from current CSV
            history_manager.update_from_csv()
            
        except ImportError:
            print("⚠ History manager not available - skipping historical import")
        except Exception as e:
            print(f"⚠ Error importing historical data: {e}")
        
        print("\n✓ Scraping completed!")
        print("\nOutput files created:")
        print("  - output/etf_dividends.csv")
        print("  - output/etf_dividends.json")
        print("  - output/etf_dividend_history_raw.csv (raw historical data)")
        print("  - output/dividend_history.json (processed history database)")
    
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()