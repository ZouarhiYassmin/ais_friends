import os
import pandas as pd
from playwright.sync_api import sync_playwright

URL = "https://www.aisfriends.com/vessels/AFRICAN-PUFFIN/9636448/311000789/76417"
CSV_FILE = "data/african_puffin_ais.csv"

os.makedirs("data", exist_ok=True)

print("Launching browser...")

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(
        user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    page.goto(URL, timeout=60000)

    #
    try:
        page.wait_for_selector("table", timeout=20000)
    except:
        print("No table found after waiting — site may have changed.")
        browser.close()
        exit(0)

    
    html = page.content()
    browser.close()

print("Page loaded. Parsing...")

from bs4 import BeautifulSoup
soup = BeautifulSoup(html, "html.parser")
tables = soup.find_all("table")

if not tables:
    print("No AIS table found.")
    exit(0)


header_row = tables[0].find("tr")
headers = [th.text.strip() for th in header_row.find_all(["th", "td"])]

rows = tables[0].find_all("tr")[1:]
new_data = []

for row in rows:
    cols = [c.text.strip() for c in row.find_all("td")]
    if cols:
        new_data.append(cols)

if not new_data:
    print("No new AIS data.")
    exit(0)

new_df = pd.DataFrame(new_data, columns=headers if headers else None)
print(f"New rows collected: {len(new_df)}")


if os.path.exists(CSV_FILE) and os.path.getsize(CSV_FILE) > 0:
    old_df = pd.read_csv(CSV_FILE)
    combined_df = pd.concat([old_df, new_df], ignore_index=True)
    combined_df = combined_df.drop_duplicates()
else:
    combined_df = new_df

combined_df.to_csv(CSV_FILE, index=False)
print(f"Dataset updated: {CSV_FILE}")
print(f"Total rows: {len(combined_df)}")
