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
CONCURRENCY_LIMIT = 30
MAX_HTTP_CLIENTS = 50
REQUEST_TIMEOUT = 20
RETRY_REQUEST_TIMEOUT = 30
MAX_FETCH_ATTEMPTS = 3
MAX_RETRY_ROUNDS = 10
BACKOFF_CAP = 10.0
PROVINCE_BATCH_SIZE = 10

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


def _backoff_delay(round_num: int) -> float:
    """Exponential backoff: 1s, 2s, 4s, ... capped at BACKOFF_CAP."""
    return min(2 ** (round_num - 2), BACKOFF_CAP)


def _retry_concurrency(round_num: int) -> int:
    """Lower concurrency on later retry rounds to reduce server load."""
    if round_num <= 3:
        return 20
    return 10

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
    """Fetch a URL with multiple retries and backoff. Returns response or None."""
    last_error = None
    for attempt in range(MAX_FETCH_ATTEMPTS):
        async with sem:
            try:
                resp = await session.get(url, headers=_HTTP_HEADERS, timeout=timeout)
                if resp.status_code == 200:
                    return resp
                last_error = f"HTTP {resp.status_code}"
            except Exception as e:
                last_error = str(e)
        if attempt < MAX_FETCH_ATTEMPTS - 1:
            await asyncio.sleep(0.5 * (attempt + 1))
    logger.warning("Failed after %d attempts: %s — %s", MAX_FETCH_ATTEMPTS, url, last_error)
    return None

# ---------------------------------------------------------------------------
# Scraping orchestration
# ---------------------------------------------------------------------------

async def scrape_all(sent_date: str) -> tuple[list[dict], int, list[dict]]:
    """Scrape all provinces and hospitals.

    Returns (rows, total_hospital_count, failed_hospitals).
    """
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    detail_tasks: list[asyncio.Task] = []
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

        async def fetch_province(
            code: int,
            use_sem: asyncio.Semaphore | None = None,
            timeout: int = REQUEST_TIMEOUT,
        ) -> tuple[int, list | None]:
            s = use_sem or sem
            resp = await _fetch(session, s, PROVINCE_URL.format(code), timeout=timeout)
            if not resp:
                return code, None
            hospitals = extract_hospital_codes(resp.text, str(code))
            nonlocal total_hospitals
            total_hospitals += len(hospitals)
            for h in hospitals:
                detail_tasks.append(asyncio.create_task(fetch_hospital_detail(h)))
            return code, hospitals

        # === Phase 1: Fetch province listings in batches with retries ===
        t0 = time.perf_counter()
        logger.info("Scraping province listings in batches of %d...", PROVINCE_BATCH_SIZE)

        remaining_provinces = list(PROVINCE_CODES)

        # First pass: batch provinces to avoid overwhelming the server
        first_pass_failed: list[int] = []
        for i in range(0, len(remaining_provinces), PROVINCE_BATCH_SIZE):
            batch = remaining_provinces[i:i + PROVINCE_BATCH_SIZE]
            results = await asyncio.gather(*[fetch_province(c) for c in batch])
            first_pass_failed.extend(code for code, hosps in results if hosps is None)
            if i + PROVINCE_BATCH_SIZE < len(remaining_provinces):
                await asyncio.sleep(0.5)

        remaining_provinces = first_pass_failed
        if remaining_provinces:
            logger.info("First pass: %d provinces failed, starting retries...", len(remaining_provinces))

        # Retry rounds with backoff
        for round_num in range(2, MAX_RETRY_ROUNDS + 1):
            if not remaining_provinces:
                break
            delay = _backoff_delay(round_num)
            concurrency = _retry_concurrency(round_num)
            retry_sem = asyncio.Semaphore(concurrency)
            logger.info(
                "Retrying %d provinces (round %d, delay %.0fs, concurrency %d)...",
                len(remaining_provinces), round_num, delay, concurrency,
            )
            await asyncio.sleep(delay)
            results = await asyncio.gather(*[
                fetch_province(c, use_sem=retry_sem, timeout=RETRY_REQUEST_TIMEOUT)
                for c in remaining_provinces
            ])
            remaining_provinces = [code for code, hosps in results if hosps is None]

        skipped_provinces = remaining_provinces
        if skipped_provinces:
            logger.warning(
                "%d provinces returned no data (may not have hospitals): %s",
                len(skipped_provinces), skipped_provinces,
            )

        t1 = time.perf_counter()
        ok_provinces = len(PROVINCE_CODES) - len(skipped_provinces)
        logger.info(
            "%d/%d provinces OK, %d hospitals found [%.1fs]. Waiting for details...",
            ok_provinces, len(PROVINCE_CODES), total_hospitals, t1 - t0,
        )

        # === Phase 2: Collect hospital detail results ===
        hospital_results = await asyncio.gather(*detail_tasks)
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

        logger.info(
            "First pass: %d OK, %d failed [%.1fs]",
            total_hospitals - len(failed_hospitals), len(failed_hospitals), t2 - t1,
        )

        # === Phase 3: Retry failed hospitals with backoff ===
        for round_num in range(2, MAX_RETRY_ROUNDS + 1):
            if not failed_hospitals:
                break

            delay = _backoff_delay(round_num)
            concurrency = _retry_concurrency(round_num)
            retry_sem = asyncio.Semaphore(concurrency)

            logger.info(
                "Retrying %d hospitals (round %d, delay %.0fs, concurrency %d)...",
                len(failed_hospitals), round_num, delay, concurrency,
            )
            await asyncio.sleep(delay)

            tr = time.perf_counter()
            retry_results = await asyncio.gather(*[
                fetch_hospital_detail(h, use_sem=retry_sem, timeout=RETRY_REQUEST_TIMEOUT)
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
        logger.info("%d/%d hospitals completed", done, total_hospitals)

        if failed_hospitals:
            logger.error("%d hospitals STILL FAILED after %d rounds:", len(failed_hospitals), MAX_RETRY_ROUNDS)
            for h in failed_hospitals:
                logger.error("  - %s (%s) [province: %s]", h["Hospital_Name"], h["kode_rs"], h["Province"])

        if empty_count:
            logger.info("%d hospitals had no bed data", empty_count)

    return all_data, total_hospitals, failed_hospitals

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
        autodetect=True,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
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

    total_start = time.perf_counter()
    sent_date = datetime.now(timezone(timedelta(hours=7))).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    all_data, total_hospitals, failed_hospitals = await scrape_all(sent_date)
    logger.info("%d rows from %d hospitals", len(all_data), total_hospitals)

    if failed_hospitals:
        logger.error(
            "ABORTING: %d/%d hospitals could not be scraped after %d retry rounds — data incomplete",
            len(failed_hospitals), total_hospitals, MAX_RETRY_ROUNDS,
        )
        return 1

    if not all_data:
        logger.error("No data scraped — aborting")
        return 1

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, write_csv, all_data)

    try:
        upload_to_bigquery(all_data, gcp_json_str)
    except Exception as e:
        logger.error("BigQuery upload failed: %s", e)
        return 1

    logger.info("Total runtime: %.1fs", time.perf_counter() - total_start)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
