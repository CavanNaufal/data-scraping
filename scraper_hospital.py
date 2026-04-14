import asyncio
import csv
import re
import time
import json
import os
from curl_cffi.requests import AsyncSession
from selectolax.parser import HTMLParser
from datetime import datetime, timedelta, timezone

_NUMBER_RE = re.compile(r'\d+')

_FIELDS = [
    'Province', 'Hospital Name', 'Class', 
    'Total Beds', 'Available Beds', 'Occupied Beds', 'BOR (%)', 
    'Sent Date'
]

PROVINCE_CODES = [
    11, 12, 13, 14, 15, 16, 17, 18, 19, 21,
    31, 32, 33, 34, 35, 36,
    51, 52, 53,
    61, 62, 63, 64, 65,
    71, 72, 73, 74, 75, 76,
    81, 82,
    91, 92, 93, 94, 95, 96, 97
]

PROVINCE_URL = 'https://keslan.kemkes.go.id/app/siranap/rumah_sakit?jenis=2&propinsi={}prop&kabkota='
HOSPITAL_URL = 'https://keslan.kemkes.go.id/app/siranap/tempat_tidur?kode_rs={}&jenis=2&propinsi={}&kabkota='

POWER_BI_URL = os.environ.get("POWER_BI_URL")

def extract_hospital_codes(html: str, prop_code: str) -> list[dict]:
    tree = HTMLParser(html)
    hospitals = []
    opt = tree.css_first(f'option[value="{prop_code}prop"]')
    prov_name = opt.text(strip=True) if opt else f"Code {prop_code}"
        
    for card in tree.css('div.cardRS'):
        h5 = card.css_first('h5')
        hosp_name = h5.text(strip=True) if h5 else "-"
        m = re.search(r'kode_rs=([A-Za-z0-9]+)', card.html)
        if m:
            hospitals.append({
                'kode_rs': m.group(1),
                'prop_code': f"{prop_code}prop",
                'Province': prov_name,
                'Hospital Name': hosp_name
            })
    return hospitals

def parse_hospital_detail(html: str, prov_name: str, hosp_name: str, wib_now: str) -> list[dict]:
    tree = HTMLParser(html)
    local_data = []
    for card in tree.css('div.card'):
        header = card.css_first('p.mb-0')
        if not header: continue
        class_name = header.text(deep=False, strip=True)
        if not class_name: class_name = header.text(strip=True).split('Update')[0].strip()
        
        number_divs = card.css('div[style*="font-size:20px"]')
        if len(number_divs) >= 2:
            try:
                t_val = _NUMBER_RE.search(number_divs[0].text(strip=True))
                a_val = _NUMBER_RE.search(number_divs[1].text(strip=True))
                total = int(t_val.group()) if t_val else 0
                avail = int(a_val.group()) if a_val else 0
                occupied = total - avail
                bor = round((occupied / total) * 100, 2) if total > 0 and occupied >= 0 else 0.0
                
                local_data.append({
                    'Province': prov_name, 'Hospital Name': hosp_name, 'Class': class_name,
                    'Total Beds': total, 'Available Beds': avail, 'Occupied Beds': max(0, occupied),
                    'BOR (%)': bor, 'Sent Date': wib_now
                })
            except: pass
    return local_data

def write_csv(all_data: list[dict]):
    with open('siranap_data.csv', 'w', newline='', encoding='utf-8') as f:
        csv.DictWriter(f, fieldnames=_FIELDS).writeheader()
        csv.DictWriter(f, fieldnames=_FIELDS).writerows(all_data)

async def _fetch(session, sem, url, timeout=20):
    for attempt in range(5):
        async with sem:
            try:
                r = await session.get(url, timeout=timeout)
                if r.status_code == 200:
                    return r
            except Exception:
                pass
        await asyncio.sleep(1.5 * (attempt + 1))
    return None

async def run():
    if not POWER_BI_URL:
        print("Error: POWER_BI_URL tidak ditemukan.")
        return
    total_start = time.perf_counter()
    wib_now = datetime.now(timezone(timedelta(hours=7))).strftime('%Y-%m-%dT%H:%M:%S.000Z')

    sem = asyncio.Semaphore(50)

    async with AsyncSession(impersonate="chrome120", max_clients=70) as session:

        MAX_ROUNDS = 5

        # -- Fase 1: ambil semua provinsi, retry maks MAX_ROUNDS ronde --
        # None = request gagal (retry), list = berhasil (bisa kosong)
        print("1. Mengambil daftar RS per provinsi...")
        remaining_provs = list(PROVINCE_CODES)
        hospital_list = []
        skipped_provs = []
        for prov_round in range(1, MAX_ROUNDS + 1):
            if not remaining_provs:
                break
            if prov_round > 1:
                print(f"   Retry provinsi ronde {prov_round}: {len(remaining_provs)} tersisa...")
                await asyncio.sleep(2.0)

            async def fetch_province(code: int):
                r = await _fetch(session, sem, PROVINCE_URL.format(code))
                if not r:
                    return code, None
                return code, extract_hospital_codes(r.text, str(code))

            results = await asyncio.gather(*[fetch_province(c) for c in remaining_provs])
            still_failed = []
            for code, hospitals in results:
                if hospitals is None:
                    still_failed.append(code)
                else:
                    hospital_list.extend(hospitals)
            remaining_provs = still_failed

        if remaining_provs:
            skipped_provs = remaining_provs
            print(f"   PERINGATAN: {len(skipped_provs)} provinsi gagal setelah {MAX_ROUNDS} ronde: {skipped_provs}")

        prov_ok = len(PROVINCE_CODES) - len(skipped_provs)
        print(f"   -> {prov_ok}/{len(PROVINCE_CODES)} provinsi OK, {len(hospital_list)} RS ditemukan.")

        # -- Fase 2: ambil detail RS, retry maks MAX_ROUNDS ronde --
        # None = request gagal (retry), list = berhasil (bisa kosong)
        print("2. Mengambil detail tempat tidur...")
        all_data = []
        remaining_hosps = list(hospital_list)
        empty_count = 0
        skipped_hosps = []
        for hosp_round in range(1, MAX_ROUNDS + 1):
            if not remaining_hosps:
                break
            if hosp_round > 1:
                print(f"   Retry RS ronde {hosp_round}: {len(remaining_hosps)} tersisa...")
                await asyncio.sleep(3.0)

            async def fetch_hosp(hosp: dict):
                r = await _fetch(session, sem, HOSPITAL_URL.format(hosp['kode_rs'], hosp['prop_code']))
                if not r:
                    return hosp, None
                return hosp, parse_hospital_detail(r.text, hosp['Province'], hosp['Hospital Name'], wib_now)

            results = await asyncio.gather(*[fetch_hosp(h) for h in remaining_hosps])
            still_failed = []
            for hosp, data in results:
                if data is None:
                    still_failed.append(hosp)
                elif data:
                    all_data.extend(data)
                else:
                    empty_count += 1
            remaining_hosps = still_failed
            done = len(hospital_list) - len(remaining_hosps)
            print(f"   ... {done}/{len(hospital_list)} RS")

        if remaining_hosps:
            skipped_hosps = remaining_hosps
            print(f"   PERINGATAN: {len(skipped_hosps)} RS gagal setelah {MAX_ROUNDS} ronde")
            for h in skipped_hosps[:10]:
                print(f"     - {h['Hospital Name']} ({h['kode_rs']})")
            if len(skipped_hosps) > 10:
                print(f"     ... dan {len(skipped_hosps) - 10} lainnya")
        if empty_count:
            print(f"   ({empty_count} RS tidak memiliki data tempat tidur)")

    print(f" -> {len(all_data)} baris dari {len(hospital_list)} RS.\nPush ke Power BI...")

    batch_size = 10000
    batches = [json.dumps(all_data[i:i+batch_size]).encode() for i in range(0, len(all_data), batch_size)]
    csv_future = asyncio.get_running_loop().run_in_executor(None, write_csv, all_data)
    async with AsyncSession(impersonate="chrome120", max_clients=5) as pbi:
        responses = await asyncio.gather(*[pbi.post(POWER_BI_URL, data=b, headers={"Content-Type":"application/json"}, timeout=40) for b in batches])
        for i, r in enumerate(responses, 1):
            if r.status_code == 200:
                print(f"   Batch {i}/{len(batches)} OK")
            else:
                print(f"   GAGAL batch {i}/{len(batches)}: HTTP {r.status_code} - {r.text[:200]}")
    await csv_future
    print(f"TOTAL RUNTIME: {time.perf_counter() - total_start:.1f}s")

if __name__ == "__main__":
    asyncio.run(run())
