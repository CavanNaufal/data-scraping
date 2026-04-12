import asyncio
import httpx
import re
import time
import pandas as pd
from selectolax.parser import HTMLParser
from datetime import datetime, timedelta, timezone

_NUMBER_RE = re.compile(r'\d+')

PROVINCE_CODES = [
    11, 12, 13, 14, 15, 16, 17, 18, 19, 21,
    31, 32, 33, 34, 35, 36,
    51, 52, 53,
    61, 62, 63, 64, 65,
    71, 72, 73, 74, 75, 76,
    81, 82,
    91, 92, 93, 94, 95, 96, 97
]

POWER_BI_URL = "https://api.powerbi.com/beta/af8e89a3-d9ac-422f-ad06-cc4eb4214314/datasets/48556833-2571-428b-a725-ffd9e90bc6e5/rows?experience=power-bi&key=Qmh7sw4QuTYGzScXKhRZi4EvslgSelbHSo5ZYuDXc9rzr7HjPt%2FTS4U9nHHuHzeMl9XPSTTpgNZHPO9H%2BcgHAg%3D%3D"

def parse_province(html: str, kode_prop: str, wib_now: str) -> list[dict]:
    tree = HTMLParser(html)
    local_data = []

    province_name = f"Code {kode_prop}"
    opt = tree.css_first(f'option[value="{kode_prop}"]')
    if opt:
        province_name = opt.text(strip=True)

    for card in tree.css('div.cardRS'):
        h5 = card.css_first('h5')
        hospital_name = h5.text(strip=True) if h5 else "-"
        for tr in card.css('table tr'):
            cols = tr.css('td')
            if len(cols) == 4:
                m = _NUMBER_RE.search(cols[2].text())
                local_data.append({
                    'Province': province_name,
                    'Hospital Name': hospital_name,
                    'Class': cols[0].text(strip=True),
                    'Room': cols[1].text(strip=True),
                    'Available Beds': int(m.group()) if m else 0,
                    'Sent Date': wib_now,
                })
    return local_data

async def run_ultra_fast_scraper():
    total_start = time.perf_counter()
    wib_now = datetime.now(timezone(timedelta(hours=7))).strftime('%Y-%m-%dT%H:%M:%S.000Z')

    # --- FASE 1: Bypass Cloudflare via Selenium (minimal) ---
    print("1. Bypass Cloudflare...")
    t0 = time.perf_counter()

    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.common.by import By

    chrome_options = Options()
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--blink-settings=imagesEnabled=false')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.page_load_strategy = 'eager'
    ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    chrome_options.add_argument(f'user-agent={ua}')

    driver = webdriver.Chrome(options=chrome_options)
    try:
        driver.get('https://keslan.kemkes.go.id/app/siranap/')
        WebDriverWait(driver, 8).until(
            lambda d: "cf_clearance" in d.execute_script("return document.cookie")
            or d.find_elements(By.CSS_SELECTOR, "select, .cardRS, #propinsi")
        )
        cookies = {c['name']: c['value'] for c in driver.get_cookies()}
    finally:
        driver.quit()

    print(f" -> Cloudflare bypass: {time.perf_counter() - t0:.1f}s")

    # --- FASE 2: Async scrape semua provinsi sekaligus ---
    print("\n2. Async scrape 38 provinsi (httpx + selectolax)...")
    t1 = time.perf_counter()

    limits = httpx.Limits(max_connections=38, max_keepalive_connections=38)
    async with httpx.AsyncClient(
        cookies=cookies,
        headers={'User-Agent': ua, 'Accept-Encoding': 'gzip, deflate, br'},
        limits=limits,
        timeout=httpx.Timeout(15.0, connect=5.0),
        http2=True,
    ) as client:

        sem = asyncio.Semaphore(38)

        async def fetch_province(code: int) -> list[dict]:
            kode = f"{code}prop"
            url = f'https://keslan.kemkes.go.id/app/siranap/rumah_sakit?jenis=2&propinsi={kode}&kabkota='
            async with sem:
                try:
                    resp = await client.get(url)
                    return parse_province(resp.text, kode, wib_now)
                except Exception as e:
                    print(f" -> Gagal {kode}: {e}")
                    return []

        tasks = [fetch_province(c) for c in PROVINCE_CODES]
        results = await asyncio.gather(*tasks)

    all_data = []
    for res in results:
        all_data.extend(res)

    print(f" -> 38 Provinsi selesai: {time.perf_counter() - t1:.1f}s")

    if not all_data:
        print("GAGAL: Datanya kosong.")
        return

    # --- FASE 3: CSV + Power BI push paralel ---
    print(f"\n3. Push {len(all_data)} baris ke Power BI...")
    t2 = time.perf_counter()

    df = pd.DataFrame(all_data)
    csv_task = asyncio.to_thread(df.to_csv, 'siranap_data.csv', index=False)

    batch_size = 5000
    batches = [all_data[i:i + batch_size] for i in range(0, len(all_data), batch_size)]

    async with httpx.AsyncClient(timeout=30.0) as pbi_client:
        push_tasks = [pbi_client.post(POWER_BI_URL, json=b) for b in batches]
        await asyncio.gather(csv_task, *push_tasks)

    print(f" -> Push selesai: {time.perf_counter() - t2:.1f}s")
    print(f"\nTOTAL RUNTIME: {time.perf_counter() - total_start:.1f}s | {len(all_data)} baris.")

if __name__ == "__main__":
    asyncio.run(run_ultra_fast_scraper())
