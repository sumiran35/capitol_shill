import pandas as pd
from playwright.sync_api import sync_playwright
from datetime import datetime, timedelta
import time
import random
import logging
from src.utils.logger import setup_logger

# Finna set up logging to catch any sus behavior
logger = setup_logger(__name__)


class CapitolTradesClient:
    """
    Scrapes capitoltrades.com directly. No API key needed.
    We stan a free data source.
    """

    BASE_URL = "https://www.capitoltrades.com/trades"

    def fetch_trades(self, start_date: str = None) -> pd.DataFrame:
        """
        Main entry point. Scrapes from start_date to Today.
        start_date format: 'YYYY-MM-DD'
        """
        today_str = datetime.now().strftime('%Y-%m-%d')

        # If no start date, we go back 90 days. No cap.
        if not start_date:
            start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')

        # Construct the URL query range: txDate=YYYY-MM-DD,YYYY-MM-DD
        # This sorts the tea by date so we get the fresh stuff first
        date_query = f"{start_date},{today_str}"
        logger.info(f"Finna scrape trades from {start_date} to {today_str}...")

        raw_data = self._run_scraper(date_query)

        if not raw_data:
            logger.warning("Scraper came back with zero riz. Empty list.")
            return pd.DataFrame()

        # Glow up the data before returning
        df = self._normalize_data(raw_data)
        return df

    def _run_scraper(self, date_range_str):
        results = []

        with sync_playwright() as p:
            # Headless=True because we ain't watching the browser do its thing
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()

            current_page = 1
            max_pages = 50  # Don't be extra, stop at 50 pages

            while current_page <= max_pages:
                # URL construction - passing the date filter to keep it 100
                url = f"{self.BASE_URL}?txDate={date_range_str}&pageSize=96&page={current_page}"
                logger.info(f"Yeeting request to Page {current_page}...")

                try:
                    page.goto(url, timeout=60000)

                    # Handle the cookie banner on page 1 or it might block our view
                    if current_page == 1:
                        try:
                            cookie_btn = page.get_by_role("button", name="Accept All")
                            if cookie_btn.is_visible(timeout=3000):
                                cookie_btn.click()
                                logger.info("Ate the cookies. Nom nom.")
                        except:
                            pass

                    # Wait for the table to load. If it ghosts us, we bounce.
                    try:
                        page.wait_for_selector("tbody tr", state="attached", timeout=10000)
                    except:
                        logger.info("Timed out waiting for data. Page is prob empty.")
                        break

                    rows = page.locator("tbody tr").all()
                    if not rows:
                        logger.info("Zero rows found. We done here.")
                        break

                    # Loop through the rows and secure the bag
                    for row in rows:
                        cells = row.locator("td").all()
                        if len(cells) < 8: continue

                        try:
                            # Extracting inner text. This is the raw tea.
                            results.append({
                                "politician_raw": cells[0].inner_text(),
                                "issuer_raw": cells[1].inner_text(),
                                "pub_date_raw": cells[2].inner_text(),
                                "trade_date_raw": cells[3].inner_text(),
                                "type_raw": cells[6].inner_text(),
                                "size_raw": cells[7].inner_text(),
                            })
                        except Exception:
                            continue  # Skip glitchy rows

                    current_page += 1
                    # Sleep a bit so we don't look sus to their WAF
                    time.sleep(random.uniform(1.0, 3.0))

                except Exception as e:
                    logger.error(f"Big L on page {current_page}: {e}")
                    break

            browser.close()

        return results

    def _normalize_data(self, raw_data: list) -> pd.DataFrame:
        """
        Takes the raw scraped JSON and gives it a glow up (Standard Schema).
        """
        df = pd.DataFrame(raw_data)
        if df.empty: return df

        # 1. Parse Politician
        # Raw: "Tina Smith\nDemocratSenateMN" -> "Tina Smith"
        df['senator'] = df['politician_raw'].apply(lambda x: x.split('\n')[0].strip())

        # 2. Parse Ticker
        # Raw: "Huntington Bancshares Inc\nHBAN:US" -> "HBAN"
        def parse_ticker(val):
            parts = val.split('\n')
            if len(parts) > 1:
                return parts[1].replace(':US', '').strip()
            return "UNKNOWN"

        df['ticker'] = df['issuer_raw'].apply(parse_ticker)
        df['asset_description'] = df['issuer_raw'].apply(lambda x: x.split('\n')[0].strip())

        # 3. Parse Dates (The tricky part)
        # Raw: "20 Nov\n2025" or "23:45\nYesterday"
        def parse_date(val):
            clean = val.replace('\n', ' ').strip()
            now = datetime.now()

            if "Yesterday" in clean:
                return now - timedelta(days=1)
            if "Today" in clean:
                return now

            # Try specific format: "20 Nov 2025"
            try:
                # remove time if present "12:00 20 Nov 2025" -> ignore time
                # Capitol trades usually is "Day Month Year"
                return pd.to_datetime(clean, format='%d %b %Y', errors='coerce')
            except:
                return pd.NaT

        df['transaction_date'] = df['trade_date_raw'].apply(parse_date)
        df['disclosure_date'] = df['pub_date_raw'].apply(parse_date)

        # 4. Parse Size (Money moves)
        # Raw: "100K–250K"
        def parse_size(val):
            if not isinstance(val, str): return 0.0
            clean = val.replace('$', '').replace(',', '').strip()

            multiplier = 1
            if 'K' in clean: multiplier = 1000
            if 'M' in clean: multiplier = 1000000

            clean = clean.replace('K', '').replace('M', '')

            # Split on en-dash or hyphen
            sep = '–' if '–' in clean else '-'
            if sep in clean:
                parts = clean.split(sep)
                try:
                    nums = [float(p) for p in parts if p]
                    return (sum(nums) / len(nums)) * multiplier
                except:
                    return 0.0
            return 0.0

        df['amount_est'] = df['size_raw'].apply(parse_size)

        # 5. Clean Metadata
        df['type'] = df['type_raw'].str.title()  # BUY -> Buy
        df['asset_type'] = 'Stock'  # Assume stock for simplicity rn
        df['sector'] = None  # Will be filled by enrichment later

        # Drop rows where we couldn't parse the date (Zombie rows)
        return df.dropna(subset=['transaction_date'])