import asyncio
import csv
import re
import time
import json
from curl_cffi.requests import AsyncSession
from selectolax.parser import HTMLParser
from datetime import datetime, timedelta, timezone

_NUMBER_RE = re.compile(r'\d+')
_FIELDS = ['Province', 'Hospital Name', 'Class', 'Room', 'Available Beds', 'Sent Date']

PROVINCE_CODES = [
    11, 12, 13, 14, 15, 16, 17, 18, 19, 21,
    31, 32, 33, 34, 35, 36,
    51, 52, 53,
    61, 62, 63, 64, 65,
    71, 72, 73, 74, 75, 76,
    81, 82,
    91, 92, 93, 94, 95, 96, 97
]

BASE_URL = 'https://keslan.kemkes.go.id/app/siranap/rumah_sakit?jenis=2&propinsi={}prop&kabkota='
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


def write_csv(all_data: list[dict]):
    with open('siranap_data.csv', 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=_FIELDS)
        w.writeheader()
        w.writerows(all_data)


async def run():
    total_start = time.perf_counter()
    wib_now = datetime.now(timezone(timedelta(hours=7))).strftime('%Y-%m-%dT%H:%M:%S.000Z')

    print("1. Scrape 38 provinsi...")
    t0 = time.perf_counter()

    async with AsyncSession(impersonate="chrome120", max_clients=50) as s:

        # Warm-up + scrape provinsi pertama berjalan BERSAMAAN
        async def warmup():
            await s.get('https://keslan.kemkes.go.id/app/siranap/', timeout=10)

        async def fetch(code: int) -> list[dict]:
            kode = f"{code}prop"
            try:
                r = await s.get(BASE_URL.format(code), timeout=12)
                return parse_province(r.text, kode, wib_now)
            except Exception as e:
                print(f" -> Gagal {kode}: {e}")
                return []

        # Fire warm-up dan semua 38 fetch sekaligus
        warmup_task = asyncio.create_task(warmup())
        fetch_tasks = [asyncio.create_task(fetch(c)) for c in PROVINCE_CODES]

        # Tunggu warm-up selesai dulu (cookie didapat), baru hasil fetch valid
        await warmup_task
        results = await asyncio.gather(*fetch_tasks)

    all_data = []
    for res in results:
        all_data.extend(res)

    print(f" -> 38 Provinsi: {time.perf_counter() - t0:.1f}s")

    if not all_data:
        print("GAGAL: Datanya kosong.")
        return

    # --- CSV + Power BI push SERENTAK ---
    total_rows = len(all_data)
    print(f"\n2. Push {total_rows} baris...")
    t1 = time.perf_counter()

    batch_size = 10000
    raw_batches = [all_data[i:i + batch_size] for i in range(0, total_rows, batch_size)]
    encoded = [json.dumps(b).encode() for b in raw_batches]

    loop = asyncio.get_running_loop()
    csv_future = loop.run_in_executor(None, write_csv, all_data)

    async with AsyncSession(impersonate="chrome120", max_clients=len(encoded) + 1) as pbi:
        push_tasks = [
            pbi.post(POWER_BI_URL, data=p, headers={"Content-Type": "application/json"}, timeout=30)
            for p in encoded
        ]
        await asyncio.gather(csv_future, *push_tasks)

    print(f" -> Push selesai: {time.perf_counter() - t1:.1f}s")
    print(f"\nTOTAL: {time.perf_counter() - total_start:.1f}s | {total_rows} baris.")


if __name__ == "__main__":
    asyncio.run(run())
