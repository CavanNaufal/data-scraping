# SIRANAP Scraper

Automated scraper untuk data ketersediaan tempat tidur rumah sakit dari [SIRANAP Kemenkes RI](https://keslan.kemkes.go.id/app/siranap/), dengan push otomatis ke Power BI streaming dataset.

## Arsitektur

```
SIRANAP Website ──> Scrapper.py ──┬──> Power BI (Streaming Dataset)
    (38 provinsi)     (async)     └──> siranap_data.csv (backup)
```

## Tech Stack

| Komponen | Library | Fungsi |
|---|---|---|
| HTTP Client | `curl-cffi` | Async HTTP dengan TLS fingerprint Chrome (bypass Cloudflare tanpa Selenium) |
| HTML Parser | `selectolax` | Parser berbasis C (lexbor), 20-30x lebih cepat dari BeautifulSoup |
| Scheduler | GitHub Actions | Cron job tiap jam |
| Package Manager | `uv` | Install dependency 10x lebih cepat dari pip |

## Data yang Dikumpulkan

| Kolom | Deskripsi |
|---|---|
| `Province` | Nama provinsi |
| `Hospital Name` | Nama rumah sakit |
| `Class` | Kelas ruangan (1, 2, 3, VIP, dll) |
| `Room` | Nama ruangan |
| `Available Beds` | Jumlah tempat tidur tersedia |
| `Sent Date` | Timestamp pengambilan data (WIB) |

## Setup Lokal

```bash
# Install dependencies
pip install -r requirements.txt

# Jalankan
python scraper.py
```

## GitHub Actions

Workflow (`Scrape.yml`) berjalan otomatis **setiap jam**. Bisa juga di-trigger manual via tab **Actions > Run workflow**.

## Struktur File

```
.
├── scraper.py          # Script utama
├── Scrape.yml           # GitHub Actions workflow
├── requirements.txt     # Python dependencies (curl-cffi, selectolax)
├── siranap_data.csv     # Output CSV (auto-generated)
└── README.MD
```

## Performance

| Metrik | Nilai |
|---|---|
| Provinsi di-scrape | 38 |
| Koneksi simultan | 50 |
| Runtime scrape | ~5-10s |
| Runtime total (termasuk push) | ~7-12s |
