#!/usr/bin/env python3
"""
ETF Dividend Scraper - Requests-based version (no Selenium required)
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import json
import re
import os
import time

class ETFDividendScraper:
    def __init__(self):
        self.etfs = [
            {'ticker': 'YBTC', 'name': 'Roundhill Bitcoin Covered Call Strategy ETF', 'frequency': 'Weekly', 'declare_day': 'Tuesday'},
            {'ticker': 'BTCI', 'name': 'Neos Bitcoin Covered Call ETF', 'frequency': 'Monthly', 'declare_day': '3rd Tuesday'},
            {'ticker': 'QQQI', 'name': 'Neos Nasdaq 100 High Income ETF', 'frequency': 'Monthly', 'declare_day': '3rd Tuesday'},
            {'ticker': 'IWMI', 'name': 'Neos Russell 2000 High Income ETF', 'frequency': 'Monthly', 'declare_day': '3rd Tuesday'},
            {'ticker': 'IAUI', 'name': 'Innovator Gold-U.S. Equity Income ETF', 'frequency': 'Monthly', 'declare_day': 'Monthly'},
            {'ticker': 'KQQQ', 'name': 'Kurv Yield Premium Strategy Nasdaq 100 ETF', 'frequency': 'Monthly', 'declare_day': 'Monthly'},
            {'ticker': 'MSTW', 'name': 'Roundhill MicroStrategy Covered Call ETF', 'frequency': 'Weekly', 'declare_day': 'Weekly'},
            {'ticker': 'WPAY', 'name': 'YieldMax Tickers PayPal Option Income ETF', 'frequency': 'Monthly', 'declare_day': 'Monthly'},
        ]

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })

    def scrape_stockanalysis(self, ticker: str) -> dict:
        """Scrape stockanalysis.com for dividend data"""
        url = f"https://stockanalysis.com/etf/{ticker.lower()}/dividend/"
        result = {'amount': None, 'ex_date': None, 'pay_date': None, 'historical': []}

        try:
            print(f"    Fetching stockanalysis.com...")
            response = self.session.get(url, timeout=15)

            if response.status_code != 200:
                print(f"      HTTP {response.status_code}")
                return result

            soup = BeautifulSoup(response.text, 'lxml')

            # Find dividend table
            table = soup.find('table')
            if not table:
                print(f"      No table found")
                return result

            rows = table.find_all('tr')[1:]  # Skip header

            for i, row in enumerate(rows[:15]):  # Get up to 15 records
                cells = row.find_all('td')
                if len(cells) >= 2:
                    ex_date = cells[0].get_text(strip=True)
                    amount_text = cells[1].get_text(strip=True)
                    amount_match = re.search(r'\$?([\d.]+)', amount_text)

                    if amount_match:
                        amount = f"${amount_match.group(1)}"
                        pay_date = cells[2].get_text(strip=True) if len(cells) >= 3 else 'N/A'

                        result['historical'].append({
                            'amount': amount,
                            'ex_date': ex_date,
                            'pay_date': pay_date
                        })

                        if i == 0:  # First row is latest
                            result['amount'] = amount
                            result['ex_date'] = ex_date
                            result['pay_date'] = pay_date

            if result['amount']:
                print(f"      Found: {result['amount']} (Ex: {result['ex_date']}) + {len(result['historical'])-1} historical")

        except Exception as e:
            print(f"      Error: {e}")

        return result

    def scrape_nasdaq(self, ticker: str) -> dict:
        """Try Nasdaq API for dividend data"""
        url = f"https://api.nasdaq.com/api/quote/{ticker}/dividends?assetclass=etf"
        result = {'amount': None, 'ex_date': None, 'pay_date': None, 'historical': []}

        try:
            print(f"    Fetching nasdaq.com API...")
            headers = self.session.headers.copy()
            headers['Accept'] = 'application/json'

            response = self.session.get(url, headers=headers, timeout=15)

            if response.status_code != 200:
                print(f"      HTTP {response.status_code}")
                return result

            data = response.json()

            if data.get('data') and data['data'].get('dividends') and data['data']['dividends'].get('rows'):
                rows = data['data']['dividends']['rows']

                for i, row in enumerate(rows[:15]):
                    ex_date = row.get('exOrEffDate', 'N/A')
                    amount = row.get('amount', 'N/A')
                    pay_date = row.get('paymentDate', 'N/A')

                    if amount and amount != 'N/A':
                        if not amount.startswith('$'):
                            amount = f"${amount}"

                        result['historical'].append({
                            'amount': amount,
                            'ex_date': ex_date,
                            'pay_date': pay_date
                        })

                        if i == 0:
                            result['amount'] = amount
                            result['ex_date'] = ex_date
                            result['pay_date'] = pay_date

                if result['amount']:
                    print(f"      Found: {result['amount']} (Ex: {result['ex_date']}) + {len(result['historical'])-1} historical")

        except Exception as e:
            print(f"      Error: {e}")

        return result

    def get_dividend_data(self, ticker: str) -> dict:
        """Try multiple sources to get dividend data"""
        print(f"  Fetching dividend data for {ticker}...")

        result = {'amount': None, 'ex_date': None, 'pay_date': None, 'historical': []}

        # Try sources in order
        sources = [self.scrape_stockanalysis, self.scrape_nasdaq]

        for source in sources:
            try:
                data = source(ticker)

                if data.get('amount') and not result['amount']:
                    result['amount'] = data['amount']
                if data.get('ex_date') and not result['ex_date']:
                    result['ex_date'] = data['ex_date']
                if data.get('pay_date') and not result['pay_date']:
                    result['pay_date'] = data['pay_date']
                if data.get('historical') and len(data['historical']) > 0:
                    result['historical'] = data['historical']

                if result['amount'] and result['ex_date'] and len(result['historical']) > 0:
                    print(f"  Complete data found with {len(result['historical'])} historical records")
                    break

                time.sleep(1)
            except Exception as e:
                print(f"    Source failed: {e}")
                continue

        if not result['amount']:
            print(f"  No data found from any source")

        return result

    def scrape_all(self) -> list:
        """Scrape dividend data for all ETFs"""
        results = []
        all_historical = []

        print("=" * 80)
        print("ETF DIVIDEND SCRAPER - Requests-based (no browser required)")
        print("Fetching current AND historical dividend data")
        print("=" * 80)

        for etf in self.etfs:
            print(f"\nProcessing {etf['ticker']} ({etf['name']})...")

            dividend_data = self.get_dividend_data(etf['ticker'])

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

            time.sleep(2)  # Be polite between ETFs

        if all_historical:
            self.save_historical_data(all_historical)

        return results

    def save_historical_data(self, historical_data: list):
        """Save historical dividend data to CSV"""
        try:
            df = pd.DataFrame(historical_data)
            filename = 'output/etf_dividend_history_raw.csv'
            df.to_csv(filename, index=False)
            print(f"\nSaved {len(historical_data)} historical dividend records to {filename}")
        except Exception as e:
            print(f"\nError saving historical data: {e}")

    def save_to_csv(self, results: list, filename: str = 'output/etf_dividends.csv'):
        """Save results to CSV"""
        df = pd.DataFrame(results)
        column_order = ['ticker', 'name', 'frequency', 'declare_day',
                       'amount', 'ex_date', 'pay_date', 'record_date', 'status', 'scraped_at']
        df = df[column_order]
        df.to_csv(filename, index=False)
        print(f"Results saved to {filename}")

    def save_to_json(self, results: list, filename: str = 'output/etf_dividends.json'):
        """Save results to JSON"""
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {filename}")

    def print_summary(self, results: list):
        """Print a formatted summary"""
        print("\n" + "=" * 100)
        print("ETF DIVIDEND DECLARATIONS SUMMARY")
        print("=" * 100)

        for row in results:
            print(f"\n{row['ticker']} - {row['name']}")
            print(f"  Frequency: {row['frequency']} (declares on {row['declare_day']})")
            print(f"  Latest Dividend: {row['amount']}")
            print(f"  Ex-Date: {row['ex_date']}")
            print(f"  Pay Date: {row['pay_date']}")
            print(f"  Status: {row['status']}")

        print("\n" + "=" * 100)
        print(f"Scrape completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 100)

        success_count = len([r for r in results if r['status'] == 'Success'])
        print(f"\nSuccess Rate: {success_count}/{len(results)} ETFs")


def main():
    os.makedirs('output', exist_ok=True)

    scraper = ETFDividendScraper()

    try:
        results = scraper.scrape_all()
        scraper.print_summary(results)
        scraper.save_to_csv(results)
        scraper.save_to_json(results)

        # Try to update history manager
        try:
            from dividend_history_manager import DividendHistoryManager
            history_manager = DividendHistoryManager()

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
                print(f"Imported {len(df_history)} historical records into database")

            history_manager.update_from_csv()
        except Exception as e:
            print(f"Warning: Could not update history: {e}")

        print("\nScraping completed!")
        print("\nOutput files:")
        print("  - output/etf_dividends.csv")
        print("  - output/etf_dividends.json")
        print("  - output/etf_dividend_history_raw.csv")

    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
