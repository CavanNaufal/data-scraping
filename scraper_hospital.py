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

async def run():
    if not POWER_BI_URL:
        print("Error: POWER_BI_URL tidak ditemukan.")
        return
    total_start = time.perf_counter()
    wib_now = datetime.now(timezone(timedelta(hours=7))).strftime('%Y-%m-%dT%H:%M:%S.000Z')

    print("1. FASE PENGINTAIAN: Mencari Kode RS...")
    hospital_list = []
    async with AsyncSession(impersonate="chrome120", max_clients=15) as s:
        sem_prov = asyncio.Semaphore(10)
        async def fetch_prov(code: int):
            async with sem_prov:
                try:
                    r = await s.get(PROVINCE_URL.format(code), timeout=30)
                    return extract_hospital_codes(r.text, str(code))
                except: return []
        prov_results = await asyncio.gather(*[fetch_prov(c) for c in PROVINCE_CODES])
        for res in prov_results: hospital_list.extend(res)

    print(f" -> Ditemukan {len(hospital_list)} RS.\n2. FASE PENYELAMAN: Ambil Detail...")
    all_data = []
    progress = 0
    async with AsyncSession(impersonate="chrome120", max_clients=25) as s2:
        sem_hosp = asyncio.Semaphore(20)
        async def fetch_hosp(hosp: dict):
            nonlocal progress
            async with sem_hosp:
                try:
                    r = await s2.get(HOSPITAL_URL.format(hosp['kode_rs'], hosp['prop_code']), timeout=25)
                    data = parse_hospital_detail(r.text, hosp['Province'], hosp['Hospital Name'], wib_now)
                    progress += 1
                    if progress % 500 == 0: print(f"    ... {progress} RS")
                    return data
                except: return []
        hosp_results = await asyncio.gather(*[fetch_hosp(h) for h in hospital_list])
        for res in hosp_results: all_data.extend(res)

    print(f"\n3. Push {len(all_data)} baris ke Power BI...")
    batch_size = 10000
    batches = [json.dumps(all_data[i:i+batch_size]).encode() for i in range(0, len(all_data), batch_size)]
    asyncio.get_running_loop().run_in_executor(None, write_csv, all_data)
    async with AsyncSession(impersonate="chrome120", max_clients=5) as pbi:
        await asyncio.gather(*[pbi.post(POWER_BI_URL, data=b, headers={"Content-Type":"application/json"}, timeout=40) for b in batches])
    print(f"TOTAL RUNTIME: {time.perf_counter() - total_start:.1f}s")

if __name__ == "__main__":
    asyncio.run(run())
