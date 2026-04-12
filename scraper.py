from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import requests
import concurrent.futures
from datetime import datetime, timedelta, timezone

def run_ultra_fast_scraper():
    print("1. Menyiapkan Bulldozer Selenium untuk menjebol Cloudflare...")
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    chrome_options.add_argument(f'user-agent={user_agent}')
    
    driver = webdriver.Chrome(options=chrome_options)
    
    # KODE PROVINSI BPS
    province_codes = [
        11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 
        31, 32, 33, 34, 35, 36, 
        51, 52, 53, 
        61, 62, 63, 64, 65, 
        71, 72, 73, 74, 75, 76, 
        81, 82, 
        91, 92, 93, 94, 95, 96, 97
    ]
    
    try:
        driver.get('https://keslan.kemkes.go.id/app/siranap/')
        print(" -> Menunggu 5 detik agar Cloudflare memberikan izin...")
        time.sleep(5)
        
        # MENCURI COOKIES (Karcis Masuk)
        selenium_cookies = driver.get_cookies()
        print(" -> Izin didapat! Mematikan Selenium...")
    finally:
        driver.quit() # Matikan browser berat ini secepatnya
        
    print("\n2. Memulai Armada Requests (Multi-Threading 10 Jalur)...")
    session = requests.Session()
    session.headers.update({'User-Agent': user_agent})
    
    # Memasukkan karcis curian tadi ke dalam sesi baru
    for cookie in selenium_cookies:
        session.cookies.set(cookie['name'], cookie['value'])

    wib_now = datetime.now(timezone(timedelta(hours=7))).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    
    # Fungsi kecil untuk mengekstrak 1 provinsi secepat kilat
    def scrape_province(prop_code):
        local_data = []
        kode_prop = f"{prop_code}prop"
        url = f'https://keslan.kemkes.go.id/app/siranap/rumah_sakit?jenis=2&propinsi={kode_prop}&kabkota='
        
        try:
            response = session.get(url, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            province_name = f"Code {kode_prop}"
            prop_option = soup.find('option', value=kode_prop)
            if prop_option:
                province_name = prop_option.text.strip()
            
            cards = soup.find_all('div', class_='cardRS')
            for card in cards:
                hospital_name = card.find('h5').text.strip() if card.find('h5') else "-"
                table = card.find('table')
                if table:
                    for tr in table.find_all('tr'):
                        columns = tr.find_all('td')
                        if len(columns) == 4:
                            availability_text = columns[2].text.strip()
                            number_match = re.search(r'\d+', availability_text)
                            just_number = number_match.group() if number_match else "0"
                            
                            local_data.append({
                                'Province': province_name,
                                'Hospital Name': hospital_name,
                                'Class': columns[0].text.strip(),
                                'Room': columns[1].text.strip(),
                                'Available Beds': int(just_number),
                                'Sent Date': wib_now
                            })
            return local_data
        except Exception as e:
            print(f" -> Gagal di {kode_prop}: {e}")
            return []

    all_data = []
    
    # PROSES MULTI-THREADING (10 Pekerja mengeksekusi 38 link secara bersamaan)
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(scrape_province, province_codes)
        for res in results:
            all_data.extend(res)
            
    end_time = time.time()
    print(f" -> 38 Provinsi selesai ditarik dalam {round(end_time - start_time, 2)} detik!\n")

    if all_data:
        # Backup CSV
        df = pd.DataFrame(all_data)
        df.to_csv('siranap_data.csv', index=False)
        
        # PUSH KE POWER BI (STRATEGI REPLACE)
        print("3. Mengirim ke Power BI...")
        POWER_BI_URL = "https://api.powerbi.com/beta/af8e89a3-d9ac-422f-ad06-cc4eb4214314/datasets/48556833-2571-428b-a725-ffd9e90bc6e5/rows?experience=power-bi&key=Qmh7sw4QuTYGzScXKhRZi4EvslgSelbHSo5ZYuDXc9rzr7HjPt%2FTS4U9nHHuHzeMl9XPSTTpgNZHPO9H%2BcgHAg%3D%3D"
        
        try:
            requests.delete(POWER_BI_URL)
        except:
            pass
        
        total_rows = len(all_data)
        batch_size = 5000 
        
        for i in range(0, total_rows, batch_size):
            batch_data = all_data[i:i + batch_size]
            try:
                requests.post(POWER_BI_URL, json=batch_data)
            except:
                pass
                
        print(f"SEMPURNA! {total_rows} baris sukses mendarat.")
    else:
        print("GAGAL: Datanya kosong.")

if __name__ == "__main__":
    run_ultra_fast_scraper()
