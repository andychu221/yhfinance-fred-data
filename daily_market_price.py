import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time
import json
import os
from pathlib import Path
import requests

class MarketDataManager:
    """
    多市場資料管理系統
    - 下載並管理各類市場資料（指數、股票、利率、外匯、商品）
    - 以 JSON 格式按資產類別分別儲存
    - 支援增量更新和資料驗證
    """

    def __init__(self, github_token=None, github_repo=None):
        """
        初始化

        Args:
            github_token: GitHub Personal Access Token
            github_repo: GitHub repository (格式: username/repo)
        """
        self.github_token = github_token
        self.github_repo = github_repo
        self.data_dir = Path('market_data')
        self.data_dir.mkdir(exist_ok=True)

        # Yahoo Finance Ticker 到 Refinitiv RIC 的映射
        self.ticker_to_ric = {
            # Equity Index
            '^GSPC': '.SPX',
            '^IXIC': '.IXIC',
            '^SOX': '.SOX',
            '^DJI': '.DJI',
            '^TWII': '.TWII',
            '000001.SS': '.SSEC',
            '000300.SS': '.CSI300',
            '^HSI': '.HSI',
            '^KS11': '.KS11',
            '^GDAXI': '.GDAXI',
            '^N225': '.N225',
            '^FCHI': '.FCHI',

            # US Stocks
            'MSFT': 'MSFT.O',
            'GOOGL': 'GOOGL.O',
            'AMZN': 'AMZN.O',
            'AAPL': 'AAPL.O',
            'NVDA': 'NVDA.O',
            'META': 'META.O',
            'TSLA': 'TSLA.O',
            'AVGO': 'AVGO.O',
            'AMD': 'AMD.O',
            'QCOM': 'QCOM.O',
            'INTC': 'INTC.O',
            'MU': 'MU.O',
            'ASML': 'ASML.O',
            'NFLX': 'NFLX.O',
            'MRVL': 'MRVL.O',
            'TSM': 'TSM.N',

            # TW Stocks
            '2330.TW': '2330.TW',
            '2317.TW': '2317.TW',
            '2454.TW': '2454.TW',
            '2308.TW': '2308.TW',
            '5347.TWO': '5347.TWO',
            '3443.TW': '3443.TW',
            '3374.TWO': '3374.TWO',
            '6789.TW': '6789.TW',

            # US Rates (FRED Series)
            'DGS3MO': 'US3MT=RR',
            'DGS2': 'US2YT=RR',
            'DGS5': 'US5YT=RR',
            'DGS10': 'US10YT=RR',
            'DGS30': 'US30YT=RR',

            # FX
            'DX-Y.NYB': '.DXY',
            'EURUSD=X': 'EUR=',
            'JPY=X': 'JPY=',
            'TWD=X': 'TWD=',
            'KRW=X': 'KRW=',
            'CNY=X': 'CNY=',

            # Commodity
            'CL=F': 'CLc1',
            'GC=F': 'GCc1'
        }

        # 定義各市場類別的商品
        self.market_data = {
            'Equity_Index': {
                '^GSPC': 'S&P 500',
                '^IXIC': 'Nasdaq',
                '^SOX': 'SOX',
                '^DJI': 'Dow Jones',
                '^TWII': 'TWSE',
                '000001.SS': 'SSE',
                '000300.SS': 'CSI300',
                '^HSI': 'HSI',
                '^KS11': 'KOSPI',
                '^GDAXI': 'DAX',
                '^N225': 'Nikkei',
                '^FCHI': 'CAC40'
            },
            'US_Stocks': {
                'MSFT': 'Microsoft',
                'GOOGL': 'Google',
                'AMZN': 'Amazon',
                'AAPL': 'Apple',
                'NVDA': 'NVIDIA',
                'META': 'Meta',
                'TSLA': 'Tesla',
                'AVGO': 'Broadcom',
                'AMD': 'AMD',
                'QCOM': 'Qualcomm',
                'INTC': 'Intel',
                'MU': 'Micron',
                'ASML': 'ASML',
                'NFLX': 'Netflix',
                'MRVL': 'Marvell',
                'TSM': 'TSMC ADR'
            },
            'TW_Stocks': {
                '2330.TW': 'TSMC',
                '2317.TW': 'Hon Hai',
                '2454.TW': 'MediaTek',
                '2308.TW': 'Delta',
                '5347.TWO': 'Vanguard',
                '3443.TW': 'Unichip',
                '3374.TWO': 'Xintec',
                '6789.TW': 'VisEra'
            },
            'US_Rates': {
                'DGS3MO': 'UST 3M',
                'DGS2': 'UST 2Y',
                'DGS5': 'UST 5Y',
                'DGS10': 'UST 10Y',
                'DGS30': 'UST 30Y'
            },
            'FX': {
                'DX-Y.NYB': 'DXY',
                'EURUSD=X': 'EUR=',
                'JPY=X': 'JPY=',
                'TWD=X': 'TWD=',
                'KRW=X': 'KRW=',
                'CNY=X': 'CNY='
            },
            'Commodity': {
                'CL=F': 'WTI Oil',
                'GC=F': 'Gold'
            }
        }

    def load_existing_data(self, market):
        """從本地或 GitHub 載入現有資料"""
        local_file = self.data_dir / f'{market}.json'

        # 先嘗試從本地載入
        if local_file.exists():
            with open(local_file, 'r') as f:
                return json.load(f)

        # 如果本地沒有，嘗試從 GitHub 下載
        if self.github_repo and self.github_token:
            try:
                url = f'https://api.github.com/repos/{self.github_repo}/contents/market_data/{market}.json'
                headers = {'Authorization': f'token {self.github_token}'}
                response = requests.get(url, headers=headers)

                if response.status_code == 200:
                    import base64
                    content = base64.b64decode(response.json()['content']).decode('utf-8')
                    data = json.loads(content)

                    # 儲存到本地
                    with open(local_file, 'w') as f:
                        json.dump(data, f)

                    return data
            except Exception as e:
                print(f"無法從 GitHub 下載 {market} 資料: {e}")

        return {}

    def download_fred_rates(self, start_date):
        """從 FRED 網站直接下載美國利率資料（CSV 格式）"""
        print("正在從 FRED 下載美國利率資料...")
        rates_data = {}

        for series_id, name in self.market_data['US_Rates'].items():
            try:
                # FRED CSV 下載 URL
                url = f'https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}'

                # 下載 CSV
                response = requests.get(url)
                response.raise_for_status()

                # 使用 pandas 讀取 CSV
                from io import StringIO
                df = pd.read_csv(StringIO(response.text))

                # 轉換日期格式並篩選起始日期之後的資料
                df.columns = ['date', 'value']
                df['date'] = pd.to_datetime(df['date'])
                df = df[df['date'] >= start_date]

                # 移除缺失值（FRED 用 "." 表示缺失）
                df['value'] = pd.to_numeric(df['value'], errors='coerce')
                df = df.dropna()

                # 轉換為字典格式 {date: value}
                if not df.empty:
                    rates_data[series_id] = {
                        str(row['date'].date()): float(row['value'])
                        for _, row in df.iterrows()
                    }
                    print(f"{name} ({series_id}): 下載了 {len(rates_data[series_id])} 筆資料")
                else:
                    print(f"{name} ({series_id}): 無資料")

                time.sleep(0.5)  # 避免請求過於頻繁

            except Exception as e:
                print(f"{name} ({series_id}) 下載失敗: {e}")

        return rates_data

    def download_yfinance_data(self, market, tickers_dict, start_date):
        """從 Yahoo Finance 下載資料"""
        print(f"正在從 Yahoo Finance 下載 {market} 資料...")
        market_data = {}

        for ticker, name in tickers_dict.items():
            try:
                data = yf.download(ticker, start=start_date, auto_adjust=True, progress=False)

                if not data.empty:
                    # 取得 Close 價格（處理可能是 Series 或 DataFrame 的情況）
                    if 'Close' in data.columns:
                        close_data = data['Close']
                    else:
                        close_data = data
                    
                    # 如果是 DataFrame（多層索引），取第一列
                    if isinstance(close_data, pd.DataFrame):
                        if close_data.shape[1] > 0:
                            close_data = close_data.iloc[:, 0]
                        else:
                            print(f"{name} ({ticker}): 無資料")
                            continue
                    
                    # 轉換為字典格式 {date: price}
                    data_dict = {}
                    for idx, value in close_data.dropna().items():
                        # 處理不同的日期格式
                        if isinstance(idx, pd.Timestamp):
                            date_str = idx.strftime('%Y-%m-%d')
                        elif hasattr(idx, 'date'):
                            date_str = str(idx.date())
                        else:
                            date_str = str(idx)
                        
                        # 確保 value 是數值
                        try:
                            data_dict[date_str] = float(value)
                        except (ValueError, TypeError):
                            continue
                    
                    if data_dict:
                        market_data[ticker] = data_dict
                        print(f"{name} ({ticker}): 下載了 {len(market_data[ticker])} 筆資料")
                    else:
                        print(f"{name} ({ticker}): 無有效資料")
                else:
                    print(f"{name} ({ticker}): 無資料")

                time.sleep(0.3)
            except Exception as e:
                print(f"{name} ({ticker}) 下載失敗: {e}")

        return market_data

    def merge_data(self, existing_data, new_data, ticker):
        """
        合併現有資料與新資料
        同一天的資料以新下載為主
        """
        if not existing_data:
            return new_data

        # 合併資料
        merged = existing_data.copy()
        merged.update(new_data)

        # 按日期排序
        merged = dict(sorted(merged.items()))

        return merged

    def convert_to_ric_format(self, market_data):
        """
        將資料轉換為使用 Refinitiv RIC 代碼的格式

        輸出格式:
        {
            "RIC_CODE": {
                "name": "商品名稱",
                "data": {
                    "2020-01-01": 100.0,
                    "2020-01-02": 101.0
                }
            }
        }
        """
        ric_data = {}

        for ticker, dates_prices in market_data.items():
            ric = self.ticker_to_ric.get(ticker, ticker)
            name = None

            # 找出對應的名稱
            for market, tickers_dict in self.market_data.items():
                if ticker in tickers_dict:
                    name = tickers_dict[ticker]
                    break

            ric_data[ric] = {
                "name": name or ticker,
                "data": dates_prices
            }

        return ric_data

    def save_to_json(self, market, data):
        """儲存資料為 JSON 格式"""
        file_path = self.data_dir / f'{market}.json'

        # 轉換為 RIC 格式
        ric_data = self.convert_to_ric_format(data)

        with open(file_path, 'w') as f:
            json.dump(ric_data, f, indent=2)

        print(f"{market} 資料已儲存至 {file_path}")
        return file_path

    def upload_to_github(self, market, file_path):
        """上傳 JSON 檔案到 GitHub"""
        if not self.github_repo or not self.github_token:
            print("GitHub 設定未完成，跳過上傳")
            return

        try:
            with open(file_path, 'r') as f:
                content = f.read()

            import base64
            content_encoded = base64.b64encode(content.encode()).decode()

            # 檢查檔案是否已存在
            url = f'https://api.github.com/repos/{self.github_repo}/contents/market_data/{market}.json'
            headers = {
                'Authorization': f'token {self.github_token}',
                'Accept': 'application/vnd.github.v3+json'
            }

            response = requests.get(url, headers=headers)

            data = {
                'message': f'Update {market} data - {datetime.now().strftime("%Y-%m-%d")}',
                'content': content_encoded
            }

            if response.status_code == 200:
                # 檔案存在，需要提供 sha
                data['sha'] = response.json()['sha']

            response = requests.put(url, headers=headers, json=data)

            if response.status_code in [200, 201]:
                print(f"{market} 已上傳至 GitHub")
            else:
                print(f"{market} 上傳失敗: {response.status_code} - {response.text}")

        except Exception as e:
            print(f"{market} 上傳時發生錯誤: {e}")

    def process_market(self, market, is_first_run=False):
        """
        處理單一市場的資料下載與更新

        Args:
            market: 市場類別名稱
            is_first_run: 是否為第一次執行
        """
        print(f"\n{'='*60}")
        print(f"處理 {market} 市場資料")
        print(f"{'='*60}")

        # 載入現有資料
        existing_data = self.load_existing_data(market)
        print(f"現有資料: {len(existing_data)} 個商品")

        # 決定下載起始日期
        if is_first_run or not existing_data:
            start_date = '2020-01-01'
            print("首次執行，從 2020-01-01 開始下載")
        else:
            # 找出現有資料中最新的日期
            latest_date = None
            for item_data in existing_data.values():
                if 'data' in item_data and item_data['data']:
                    item_latest_date_str = max(item_data['data'].keys())
                    item_latest_date = datetime.strptime(item_latest_date_str, '%Y-%m-%d')
                    if latest_date is None or item_latest_date > latest_date:
                        latest_date = item_latest_date

            if latest_date:
                # 從最新日期的下一天開始
                start_date = (latest_date + timedelta(days=-1)).strftime('%Y-%m-%d')
                print(f"增量更新，從 {start_date} 開始下載")
            else:
                # 如果沒有現有資料，從 5 年前開始
                start_date = (datetime.now() - timedelta(days=365*5)).strftime('%Y-%m-%d')
                print(f"現有資料為空，從 {start_date} 開始下載")

        # 下載新資料
        if market == 'US_Rates':
            new_data = self.download_fred_rates(start_date)
        else:
            tickers_dict = self.market_data[market]
            new_data = self.download_yfinance_data(market, tickers_dict, start_date)

        # 合併資料
        merged_data = {}
        for ticker, new_prices in new_data.items():
            # 使用 RIC 代碼找到現有資料
            ric = self.ticker_to_ric.get(ticker, ticker)
            old_prices = existing_data.get(ric, {}).get('data', {})

            merged_data[ticker] = self.merge_data(old_prices, new_prices, ticker)

        # 儲存資料
        if merged_data:
            file_path = self.save_to_json(market, merged_data)

            # 上傳到 GitHub
            self.upload_to_github(market, file_path)

            print(f"\n{market} 統計:")
            print(f"  - 商品數量: {len(merged_data)}")
            total_records = sum(len(dates) for dates in merged_data.values())
            print(f"  - 總記錄數: {total_records}")
        else:
            print(f"{market} 沒有資料可儲存")

    def run(self, is_first_run=False):
        """
        執行完整的資料下載與更新流程

        Args:
            is_first_run: 是否為第一次執行
        """
        print("="*60)
        print("多市場資料管理系統")
        print("="*60)
        print(f"執行時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"模式: {'首次下載 (2020-01-01 至今)' if is_first_run else '增量更新 (過去5年)'}")
        print("="*60)

        # 處理各市場
        for market in self.market_data.keys():
            try:
                self.process_market(market, is_first_run)
            except Exception as e:
                print(f"{market} 處理失敗: {e}")
                continue

        print("\n" + "="*60)
        print("所有市場資料處理完成！")
        print("="*60)


# 使用範例
if __name__ == "__main__":
    # 設定參數
    GITHUB_TOKEN = ""  # 從 GitHub Settings > Developer settings > Personal access tokens 取得
    GITHUB_REPO = "andychu221/yhfinance-fred-data"  # 你的 GitHub repository

    # 建立管理器
    manager = MarketDataManager(
        github_token=GITHUB_TOKEN,
        github_repo=GITHUB_REPO
    )

    # 首次執行設定為 True，之後設定為 False
    IS_FIRST_RUN = False

    # 執行
    manager.run(is_first_run=IS_FIRST_RUN)
