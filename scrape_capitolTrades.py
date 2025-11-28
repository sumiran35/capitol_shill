from playwright.sync_api import sync_playwright
import json
import time
import random


def scrape_capitol_trades_90d():
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        print("Starting scraper for the last 90 days...")

        current_page = 1
        # 90 days is usually around 5-10 pages, but we set 50 to be safe.
        # The script will auto-stop when it hits an empty page.
        max_pages = 50

        while current_page <= max_pages:
            # UPDATED URL: Added 'txDate=90d' to filter results
            url = f"https://www.capitoltrades.com/trades?txDate=90d&pageSize=96&page={current_page}"
            print(f"Scraping {url} ...")

            try:
                page.goto(url)

                # Handle Cookie Banner (First page only)
                if current_page == 1:
                    try:
                        cookie_button = page.get_by_role("button", name="Accept All")
                        if cookie_button.is_visible(timeout=3000):
                            cookie_button.click()
                            print("Accepted cookies.")
                    except:
                        pass

                # Wait for table data
                try:
                    page.wait_for_selector("tbody tr", timeout=8000)
                except:
                    print("Timed out waiting for data. Reached the end or page is empty.")
                    break

                # Extract Rows
                rows = page.locator("tbody tr").all()
                row_count = len(rows)
                print(f"  - Found {row_count} trades.")

                # STOP CONDITION: If no rows are found, we are done.
                if row_count == 0:
                    print("No trades found on this page. Stopping.")
                    break

                for row in rows:
                    cells = row.locator("td").all()
                    if len(cells) < 8: continue

                    try:
                        trade_data = {
                            "politician": cells[0].inner_text().split('\n')[0],
                            "party_state": cells[0].inner_text().split('\n')[1] if '\n' in cells[
                                0].inner_text() else "",
                            "issuer": cells[1].inner_text().split('\n')[0],
                            "ticker": cells[1].inner_text().split('\n')[1] if '\n' in cells[1].inner_text() else "",
                            "pub_date": cells[2].inner_text(),
                            "trade_date": cells[3].inner_text(),
                            "filed_after": cells[4].inner_text().replace("days", "").strip(),
                            "owner": cells[5].inner_text(),
                            "type": cells[6].inner_text(),
                            "size": cells[7].inner_text(),
                            "price": cells[8].inner_text()
                        }
                        results.append(trade_data)
                    except Exception:
                        continue

                current_page += 1
                time.sleep(random.uniform(1.0, 2.0))

            except Exception as e:
                print(f"Error on page {current_page}: {e}")
                break

        browser.close()

    # Save to JSON
    filename = "capitol_trades_90d.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4)

    print(f"Done! Scraped {len(results)} trades total.")


if __name__ == "__main__":
    scrape_capitol_trades_90d()