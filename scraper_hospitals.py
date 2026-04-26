import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

from curl_cffi.requests import AsyncSession
from google.cloud import bigquery
from google.oauth2 import service_account
from selectolax.parser import HTMLParser

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CONCURRENCY_LIMIT = 100
MAX_HTTP_CLIENTS = 120
REQUEST_TIMEOUT = 15
RETRY_TIMEOUT = 30
RETRY_CONCURRENCY = 20
MAX_RETRY_ROUNDS = 3
RETRY_DELAY = 1.5
VERIFY_TIMEOUT = 30
VERIFY_CONCURRENCY = 15

BQ_DATASET = "siranap_db"
BQ_TABLE = "bed_capacity"
CSV_OUTPUT = "siranap_data.csv"

FIELDS = [
    "Province", "Kode_RS", "Hospital_Name", "Class",
    "Total_Beds", "Available_Beds", "Occupied_Beds", "BOR_Percentage",
    "Sent_Date",
]

PROVINCE_CODES = [
    11, 12, 13, 14, 15, 16, 17, 18, 19, 21,
    31, 32, 33, 34, 35, 36,
    51, 52, 53,
    61, 62, 63, 64, 65,
    71, 72, 73, 74, 75, 76,
    81, 82,
    91, 92, 93, 94, 95, 96, 97,
]

PROVINCE_URL = (
    "https://keslan.kemkes.go.id/app/siranap/rumah_sakit"
    "?jenis=2&propinsi={}prop&kabkota="
)
HOSPITAL_URL = (
    "https://keslan.kemkes.go.id/app/siranap/tempat_tidur"
    "?kode_rs={}&jenis=2&propinsi={}&kabkota="
)

_HTTP_HEADERS = {
    "Accept": "text/html",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "id-ID,id;q=0.9",
    "Connection": "keep-alive",
}

_NUMBER_RE = re.compile(r"\d+")

# ---------------------------------------------------------------------------
# HTML Parsing
# ---------------------------------------------------------------------------

def extract_hospital_codes(html: str, prop_code: str) -> list[dict]:
    """Parse province listing page and return hospital metadata."""
    tree = HTMLParser(html)
    opt = tree.css_first(f'option[value="{prop_code}prop"]')
    prov_name = opt.text(strip=True) if opt else f"Code {prop_code}"

    hospitals: list[dict] = []
    for card in tree.css("div.cardRS"):
        h5 = card.css_first("h5")
        hosp_name = h5.text(strip=True) if h5 else "-"
        match = re.search(r"kode_rs=([A-Za-z0-9]+)", card.html)
        if match:
            hospitals.append({
                "kode_rs": match.group(1),
                "prop_code": f"{prop_code}prop",
                "Province": prov_name,
                "Hospital_Name": hosp_name,
            })
    return hospitals


def parse_hospital_detail(
    html: str,
    prov_name: str,
    hosp_name: str,
    sent_date: str,
    kode_rs: str,
) -> list[dict]:
    """Parse hospital detail page and return bed-capacity rows."""
    tree = HTMLParser(html)
    rows: list[dict] = []

    for card in tree.css("div.card"):
        header = card.css_first("p.mb-0")
        if not header:
            continue

        class_name = header.text(deep=False, strip=True)
        if not class_name:
            class_name = header.text(strip=True).split("Update")[0].strip()

        number_divs = card.css('div[style*="font-size:20px"]')
        if len(number_divs) < 2:
            continue

        try:
            t_val = _NUMBER_RE.search(number_divs[0].text(strip=True))
            a_val = _NUMBER_RE.search(number_divs[1].text(strip=True))
            total = int(t_val.group()) if t_val else 0
            avail = int(a_val.group()) if a_val else 0
            occupied = total - avail
            bor = round((occupied / total) * 100, 2) if total > 0 and occupied >= 0 else 0.0

            rows.append({
                "Province": prov_name,
                "Kode_RS": kode_rs,
                "Hospital_Name": hosp_name,
                "Class": class_name,
                "Total_Beds": total,
                "Available_Beds": avail,
                "Occupied_Beds": max(0, occupied),
                "BOR_Percentage": bor,
                "Sent_Date": sent_date,
            })
        except Exception as e:
            logger.warning("Parse error for %s (%s), class '%s': %s", hosp_name, kode_rs, class_name, e)

    return rows

# ---------------------------------------------------------------------------
# HTTP Fetching
# ---------------------------------------------------------------------------

async def _fetch(
    session: AsyncSession,
    sem: asyncio.Semaphore,
    url: str,
    timeout: int = REQUEST_TIMEOUT,
) -> object | None:
    """Fetch a URL with one retry. Returns response or None."""
    for attempt in range(2):
        async with sem:
            try:
                resp = await session.get(url, headers=_HTTP_HEADERS, timeout=timeout)
                if resp.status_code == 200:
                    return resp
            except Exception:
                pass
        if attempt == 0:
            await asyncio.sleep(0.15)
    return None

# ---------------------------------------------------------------------------
# BigQuery: check if today already has data
# ---------------------------------------------------------------------------

def get_today_hospital_count(client: bigquery.Client, table_id: str, today_wib: str) -> int:
    """Get the number of unique hospitals already stored for today in BigQuery."""
    query = f"""
        SELECT COUNT(DISTINCT CONCAT(Province, '||', Hospital_Name)) as hospital_count
        FROM `{table_id}`
        WHERE DATE(Sent_Date) = '{today_wib}'
    """
    try:
        result = list(client.query(query).result())
        count = result[0].hospital_count if result else 0
        logger.info("Existing data for %s: %d hospitals in BigQuery", today_wib, count)
        return count
    except Exception as e:
        logger.warning("Could not check existing data: %s — proceeding with scrape", e)
        return 0


def delete_today_data(client: bigquery.Client, table_id: str, today_wib: str) -> None:
    """Delete all rows for today's date from BigQuery."""
    query = f"""
        DELETE FROM `{table_id}`
        WHERE DATE(Sent_Date) = '{today_wib}'
    """
    job = client.query(query)
    job.result()
    logger.info("Deleted existing data for %s from BigQuery", today_wib)

# ---------------------------------------------------------------------------
# Scraping orchestration
# ---------------------------------------------------------------------------

async def scrape_all(sent_date: str) -> tuple[list[dict], int, list[dict]]:
    """Scrape all provinces and hospitals.

    Returns (rows, total_hospital_count, failed_hospitals).
    """
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    all_results: list[asyncio.Task] = []
    total_hospitals = 0

    async with AsyncSession(impersonate="chrome120", max_clients=MAX_HTTP_CLIENTS) as session:

        async def fetch_hospital_detail(
            hosp: dict,
            use_sem: asyncio.Semaphore | None = None,
            timeout: int = REQUEST_TIMEOUT,
        ) -> tuple[dict, list | None]:
            s = use_sem or sem
            resp = await _fetch(session, s, HOSPITAL_URL.format(hosp["kode_rs"], hosp["prop_code"]), timeout=timeout)
            if not resp:
                return hosp, None
            return hosp, parse_hospital_detail(resp.text, hosp["Province"], hosp["Hospital_Name"], sent_date, hosp["kode_rs"])

        async def fetch_province(code: int) -> tuple[int, list | None]:
            resp = await _fetch(session, sem, PROVINCE_URL.format(code))
            if not resp:
                return code, None
            hospitals = extract_hospital_codes(resp.text, str(code))
            nonlocal total_hospitals
            total_hospitals += len(hospitals)
            for h in hospitals:
                all_results.append(asyncio.create_task(fetch_hospital_detail(h)))
            return code, hospitals

        t0 = time.perf_counter()
        logger.info("Scraping provinces + hospital details (concurrency=%d)...", CONCURRENCY_LIMIT)

        # Fetch all provinces at once (fast-fail approach)
        prov_remaining = list(PROVINCE_CODES)
        for round_num in range(1, MAX_RETRY_ROUNDS + 1):
            if not prov_remaining:
                break
            if round_num > 1:
                logger.info("Retrying %d provinces (round %d)...", len(prov_remaining), round_num)
                await asyncio.sleep(RETRY_DELAY)
            results = await asyncio.gather(*[fetch_province(c) for c in prov_remaining])
            prov_remaining = [code for code, hosps in results if hosps is None]

        if prov_remaining:
            logger.warning("%d provinces returned no data: %s", len(prov_remaining), prov_remaining)

        t1 = time.perf_counter()
        ok_prov = len(PROVINCE_CODES) - len(prov_remaining)
        logger.info("%d/%d provinces, %d hospitals [%.1fs]. Waiting for details...", ok_prov, len(PROVINCE_CODES), total_hospitals, t1 - t0)

        # Collect hospital results
        hospital_results = await asyncio.gather(*all_results)
        t2 = time.perf_counter()

        all_data: list[dict] = []
        empty_count = 0
        failed_hospitals: list[dict] = []

        for hosp, data in hospital_results:
            if data is None:
                failed_hospitals.append(hosp)
            elif data:
                all_data.extend(data)
            else:
                empty_count += 1

        logger.info("First pass: %d OK, %d failed [%.1fs]", total_hospitals - len(failed_hospitals), len(failed_hospitals), t2 - t1)

        # Retry failed hospitals with lower concurrency and longer timeout
        retry_sem = asyncio.Semaphore(RETRY_CONCURRENCY)
        for round_num in range(2, MAX_RETRY_ROUNDS + 1):
            if not failed_hospitals:
                break
            logger.info("Retrying %d hospitals (round %d, concurrency=%d, timeout=%ds)...", len(failed_hospitals), round_num, RETRY_CONCURRENCY, RETRY_TIMEOUT)
            await asyncio.sleep(RETRY_DELAY)
            tr = time.perf_counter()
            retry_results = await asyncio.gather(*[
                fetch_hospital_detail(h, use_sem=retry_sem, timeout=RETRY_TIMEOUT)
                for h in failed_hospitals
            ])
            still_failed: list[dict] = []
            for hosp, data in retry_results:
                if data is None:
                    still_failed.append(hosp)
                elif data:
                    all_data.extend(data)
                else:
                    empty_count += 1
            recovered = len(failed_hospitals) - len(still_failed)
            logger.info("  +%d recovered [%.1fs]", recovered, time.perf_counter() - tr)
            failed_hospitals = still_failed

        done = total_hospitals - len(failed_hospitals)
        logger.info("%d/%d hospitals completed after retries", done, total_hospitals)

        # === Phase 4: Verify remaining failures one-by-one ===
        # Distinguish "no bed data" from "truly unreachable"
        truly_failed: list[dict] = []
        if failed_hospitals:
            verify_sem = asyncio.Semaphore(VERIFY_CONCURRENCY)
            logger.info("Verifying %d remaining hospitals one-by-one (timeout=%ds)...", len(failed_hospitals), VERIFY_TIMEOUT)
            await asyncio.sleep(2)

            async def verify_hospital(hosp: dict) -> tuple[dict, str, list]:
                """Returns (hosp, status, data). Status: 'ok', 'no_data', or error description."""
                url = HOSPITAL_URL.format(hosp["kode_rs"], hosp["prop_code"])
                async with verify_sem:
                    try:
                        resp = await session.get(url, headers=_HTTP_HEADERS, timeout=VERIFY_TIMEOUT)
                        if resp.status_code == 200:
                            data = parse_hospital_detail(resp.text, hosp["Province"], hosp["Hospital_Name"], sent_date, hosp["kode_rs"])
                            if data:
                                return hosp, "ok", data
                            return hosp, "no_data", []
                        return hosp, f"http_{resp.status_code}", []
                    except Exception as e:
                        return hosp, f"error: {e}", []

            verify_results = await asyncio.gather(*[verify_hospital(h) for h in failed_hospitals])

            for hosp, status, data in verify_results:
                if status == "ok":
                    all_data.extend(data)
                    logger.info("  Verified OK: %s (%s)", hosp["Hospital_Name"], hosp["kode_rs"])
                elif status == "no_data":
                    empty_count += 1
                    logger.info("  No bed data: %s (%s) — skipping (normal)", hosp["Hospital_Name"], hosp["kode_rs"])
                else:
                    truly_failed.append(hosp)
                    logger.warning("  Unreachable: %s (%s) — %s", hosp["Hospital_Name"], hosp["kode_rs"], status)

        final_done = total_hospitals - len(truly_failed)
        logger.info("Final: %d/%d hospitals completed", final_done, total_hospitals)

        if truly_failed:
            logger.error("%d hospitals truly unreachable:", len(truly_failed))
            for h in truly_failed:
                logger.error("  - %s (%s) [%s]", h["Hospital_Name"], h["kode_rs"], h["Province"])

        if empty_count:
            logger.info("%d hospitals had no bed data (normal)", empty_count)

    return all_data, total_hospitals, truly_failed

# ---------------------------------------------------------------------------
# Output: CSV
# ---------------------------------------------------------------------------

def write_csv(all_data: list[dict]) -> None:
    """Write scraped data to CSV file."""
    with open(CSV_OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(all_data)
    logger.info("CSV saved: %s (%d rows)", CSV_OUTPUT, len(all_data))

# ---------------------------------------------------------------------------
# Output: BigQuery
# ---------------------------------------------------------------------------

def upload_to_bigquery(all_data: list[dict], gcp_json_str: str) -> None:
    """Load scraped data into BigQuery via append."""
    gcp_info = json.loads(gcp_json_str)
    credentials = service_account.Credentials.from_service_account_info(gcp_info)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    table_id = f"{credentials.project_id}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("Province", "STRING"),
            bigquery.SchemaField("Kode_RS", "STRING"),
            bigquery.SchemaField("Hospital_Name", "STRING"),
            bigquery.SchemaField("Class", "STRING"),
            bigquery.SchemaField("Total_Beds", "INTEGER"),
            bigquery.SchemaField("Available_Beds", "INTEGER"),
            bigquery.SchemaField("Occupied_Beds", "INTEGER"),
            bigquery.SchemaField("BOR_Percentage", "FLOAT"),
            bigquery.SchemaField("Sent_Date", "TIMESTAMP"),
        ],
    )

    bq_start = time.perf_counter()
    job = client.load_table_from_json(all_data, table_id, job_config=job_config)
    job.result()

    logger.info("BigQuery load complete: %d rows -> %s [%.1fs]", job.output_rows, table_id, time.perf_counter() - bq_start)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run() -> int:
    """Main orchestrator. Returns 0 on success, 1 on failure."""
    gcp_json_str = os.environ.get("GCP_CREDENTIALS")
    if not gcp_json_str:
        logger.error("Environment variable GCP_CREDENTIALS is not set")
        return 1

    # Check if today's data already exists in BigQuery
    wib_tz = timezone(timedelta(hours=7))
    today_wib = datetime.now(wib_tz).strftime("%Y-%m-%d")

    try:
        gcp_info = json.loads(gcp_json_str)
        credentials = service_account.Credentials.from_service_account_info(gcp_info)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        table_id = f"{credentials.project_id}.{BQ_DATASET}.{BQ_TABLE}"

        existing_hospital_count = get_today_hospital_count(client, table_id, today_wib)
    except Exception as e:
        logger.warning("Could not verify existing data: %s — proceeding with scrape", e)
        existing_hospital_count = 0

    total_start = time.perf_counter()
    sent_date = datetime.now(wib_tz).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    all_data, total_hospitals, failed_hospitals = await scrape_all(sent_date)
    logger.info("%d rows from %d hospitals", len(all_data), total_hospitals)

    if failed_hospitals:
        logger.warning(
            "%d/%d hospitals could not be scraped — continuing with partial data",
            len(failed_hospitals), total_hospitals,
        )

    if not all_data:
        logger.error("No data scraped — aborting")
        return 1

    # Compare with existing data: only upload if we have more hospitals
    if total_hospitals <= existing_hospital_count:
        logger.info(
            "Scrape got %d hospitals, existing data has %d — keeping existing data (skip upload)",
            total_hospitals, existing_hospital_count,
        )
        return 0

    logger.info(
        "Scrape got %d hospitals, existing data has %d — replacing with better data",
        total_hospitals, existing_hospital_count,
    )

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, write_csv, all_data)

    try:
        if existing_hospital_count > 0:
            delete_today_data(client, table_id, today_wib)
        upload_to_bigquery(all_data, gcp_json_str)
    except Exception as e:
        logger.error("BigQuery upload failed: %s", e)
        return 1

    logger.info("Total runtime: %.1fs", time.perf_counter() - total_start)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
