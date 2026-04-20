import asyncio
import csv
import re
import time
import json
import os
from curl_cffi.requests import AsyncSession
from selectolax.parser import HTMLParser
from datetime import datetime, timedelta, timezone

# --- IMPORT UNTUK GOOGLE BIGQUERY ---
from google.oauth2 import service_account
from google.cloud import bigquery

_NUMBER_RE = re.compile(r'\d+')

# Kolom sudah distandarisasi (Tanpa spasi & simbol) untuk BigQuery
_FIELDS = [
    'Province', 'Hospital_Name', 'Class', 
    'Total_Beds', 'Available_Beds', 'Occupied_Beds', 'BOR_Percentage', 
    'Sent_Date'
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
                'Hospital_Name': hosp_name
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
                    'Province': prov_name, 'Hospital_Name': hosp_name, 'Class': class_name,
                    'Total_Beds': total, 'Available_Beds': avail, 'Occupied_Beds': max(0, occupied),
                    'BOR_Percentage': bor, 'Sent_Date': wib_now
                })
            except: pass
    return local_data

def write_csv(all_data: list[dict]):
    with open('siranap_data.csv', 'w', newline='', encoding='utf-8') as f:
        csv.DictWriter(f, fieldnames=_FIELDS).writeheader()
        csv.DictWriter(f, fieldnames=_FIELDS).writerows(all_data)

MAX_ROUNDS = 5

_HEADERS = {
    "Accept": "text/html",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "id-ID,id;q=0.9",
    "Connection": "keep-alive",
}

async def _fetch(session, sem, url, timeout=15):
    for attempt in range(2):
        async with sem:
            try:
                r = await session.get(url, headers=_HEADERS, timeout=timeout)
                if r.status_code == 200:
                    return r
            except Exception:
                pass
        if attempt == 0:
            await asyncio.sleep(0.3)
    return None

async def run():
    # --- CEK KUNCI RAHASIA GCP ---
    gcp_json_str = os.environ.get("GCP_CREDENTIALS")
    if not gcp_json_str:
        print("Error: Variabel environment GCP_CREDENTIALS tidak ditemukan.")
        return

    total_start = time.perf_counter()
    wib_now = datetime.now(timezone(timedelta(hours=7))).strftime('%Y-%m-%dT%H:%M:%S.000Z')

    sem = asyncio.Semaphore(100)
    all_results: list[asyncio.Task] = []
    total_hosps = [0]

    async with AsyncSession(impersonate="chrome120", max_clients=120) as session:

        async def fetch_hosp(hosp: dict) -> tuple[dict, list | None]:
            r = await _fetch(session, sem, HOSPITAL_URL.format(hosp['kode_rs'], hosp['prop_code']))
            if not r:
                return hosp, None
            return hosp, parse_hospital_detail(r.text, hosp['Province'], hosp['Hospital_Name'], wib_now)

        async def fetch_province(code: int):
            r = await _fetch(session, sem, PROVINCE_URL.format(code))
            if not r:
                return code, None
            hospitals = extract_hospital_codes(r.text, str(code))
            total_hosps[0] += len(hospitals)
            for h in hospitals:
                all_results.append(asyncio.create_task(fetch_hosp(h)))
            return code, hospitals

        t0 = time.perf_counter()
        print("Scraping provinsi + detail RS (pipeline)...")
        prov_remaining = list(PROVINCE_CODES)
        prov_skipped = []
        for ronde in range(1, MAX_ROUNDS + 1):
            if not prov_remaining:
                break
            if ronde > 1:
                print(f"   Retry {len(prov_remaining)} provinsi (ronde {ronde})...")
                await asyncio.sleep(1.0)
            results = await asyncio.gather(*[fetch_province(c) for c in prov_remaining])
            prov_remaining = [code for code, hosps in results if hosps is None]

        if prov_remaining:
            prov_skipped = prov_remaining
            print(f"   PERINGATAN: {len(prov_skipped)} provinsi gagal: {prov_skipped}")
        t1 = time.perf_counter()
        print(f"   -> {len(PROVINCE_CODES) - len(prov_skipped)}/{len(PROVINCE_CODES)} provinsi, {total_hosps[0]} RS [{t1-t0:.1f}s]. Menunggu detail...")

        hosp_results = await asyncio.gather(*all_results)
        t2 = time.perf_counter()

        all_data = []
        empty_count = 0
        failed_hosps = []
        for hosp, data in hosp_results:
            if data is None:
                failed_hosps.append(hosp)
            elif data:
                all_data.extend(data)
            else:
                empty_count += 1
        print(f"   Pass pertama: {total_hosps[0]-len(failed_hosps)} OK, {len(failed_hosps)} gagal [{t2-t1:.1f}s]")

        for ronde in range(2, MAX_ROUNDS + 1):
            if not failed_hosps:
                break
            tr = time.perf_counter()
            print(f"   Retry {len(failed_hosps)} RS (ronde {ronde})...")
            await asyncio.sleep(0.5)
            retry_results = await asyncio.gather(*[fetch_hosp(h) for h in failed_hosps])
            still_failed = []
            for hosp, data in retry_results:
                if data is None:
                    still_failed.append(hosp)
                elif data:
                    all_data.extend(data)
                else:
                    empty_count += 1
            recovered = len(failed_hosps) - len(still_failed)
            print(f"     +{recovered} recovered [{time.perf_counter()-tr:.1f}s]")
            failed_hosps = still_failed

        done = total_hosps[0] - len(failed_hosps)
        print(f"   ... {done}/{total_hosps[0]} RS berhasil")
        if failed_hosps:
            print(f"   PERINGATAN: {len(failed_hosps)} RS gagal setelah {MAX_ROUNDS} ronde")
            for h in failed_hosps[:10]:
                print(f"     - {h['Hospital_Name']} ({h['kode_rs']})")
            if len(failed_hosps) > 10:
                print(f"     ... dan {len(failed_hosps) - 10} lainnya")
        if empty_count:
            print(f"   ({empty_count} RS tidak memiliki data tempat tidur)")

    print(f" -> {len(all_data)} baris dari {total_hosps[0]} RS.\nMenyimpan ke CSV dan Load ke BigQuery...")

    # 1. Simpan CSV lokal (sebagai cadangan di GitHub)
    csv_future = asyncio.get_running_loop().run_in_executor(None, write_csv, all_data)
    await csv_future

    # 2. Load ke Google BigQuery
    try:
        gcp_info = json.loads(gcp_json_str)
        credentials = service_account.Credentials.from_service_account_info(gcp_info)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        
        # Format tabel: "project_id.dataset_id.table_id"
        table_id = f"{credentials.project_id}.siranap_db.bed_capacity"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND, # Mode tambah data (tidak menimpa)
            autodetect=True, # Biarkan BigQuery mendeteksi tipe kolom otomatis
        )
        
        bq_start = time.perf_counter()
        job = client.load_table_from_json(all_data, table_id, job_config=job_config)
        job.result() # Tunggu hingga selesai
        
        print(f"✅ BERHASIL: {job.output_rows} baris masuk ke tabel BigQuery {table_id}.")
        print(f"   Waktu load BQ: {time.perf_counter() - bq_start:.1f}s")
    except Exception as e:
        print(f"❌ GAGAL load ke BigQuery: {e}")

    print(f"TOTAL RUNTIME: {time.perf_counter() - total_start:.1f}s")

if __name__ == "__main__":
    asyncio.run(run())
