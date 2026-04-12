from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import requests
import json
from datetime import datetime, timedelta, timezone

def run_fast_scraper():
    print("Setting up headless browser...")
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    driver = webdriver.Chrome(options=chrome_options)
    all_data = []
    
    # Timestamp WIB (UTC+7)
    wib_now = datetime.now(timezone(timedelta(hours=7))).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    
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
        print("Warming up the browser...")
        driver.get('https://keslan.kemkes.go.id/app/siranap/')
        time.sleep(10)

        for i in province_codes:
            prop_code = f"{i}prop"
            url = f'https://keslan.kemkes.go.id/app/siranap/rumah_sakit?jenis=2&propinsi={prop_code}&kabkota='
            driver.get(url)
            
            try:
                WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.CLASS_NAME, "cardRS")))
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                province_name = f"Code {prop_code}"
                prop_option = soup.find('option', value=prop_code)
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
                                
                                all_data.append({
                                    'Province': province_name,
                                    'Hospital Name': hospital_name,
                                    'Class': columns[0].text.strip(),
                                    'Room': columns[1].text.strip(),
                                    'Available Beds': int(just_number),
                                    'Sent Date': wib_now
                                })
            except TimeoutException:
                pass 
            
    finally:
        driver.quit()
        
    if all_data:
        # URL API POWER BI ANDA
        POWER_BI_URL = "https://api.powerbi.com/beta/af8e89a3-d9ac-422f-ad06-cc4eb4214314/datasets/48556833-2571-428b-a725-ffd9e90bc6e5/rows?experience=power-bi&key=Qmh7sw4QuTYGzScXKhRZi4EvslgSelbHSo5ZYuDXc9rzr7HjPt%2FTS4U9nHHuHzeMl9XPSTTpgNZHPO9H%2BcgHAg%3D%3D"

        # --- LANGKAH STRATEGI REPLACE: HAPUS DATA LAMA ---
        print("Menyapu bersih data lama di Power BI...")
        try:
            # Mengirim request DELETE untuk mengosongkan dataset
            response_del = requests.delete(POWER_BI_URL)
            if response_del.status_code == 200:
                print("Data lama berhasil dihapus. Memulai pengiriman data segar...")
            else:
                print(f"Catatan: Perintah hapus merespon {response_del.status_code}. Melanjutkan pengiriman.")
        except Exception as e:
            print(f"Gagal perintah hapus: {e}. Melanjutkan pengiriman.")
        
        time.sleep(2) # Jeda singkat setelah penghapusan
        # ------------------------------------------------

        total_rows = len(all_data)
        batch_size = 5000 
        
        for i in range(0, total_rows, batch_size):
            batch_data = all_data[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            print(f"Mengirim Paket {batch_number}...")
            
            try:
                response = requests.post(POWER_BI_URL, json=batch_data)
                if response.status_code == 200:
                    print(f" -> Paket {batch_number} SUKSES.")
                else:
                    print(f" -> Paket {batch_number} GAGAL ({response.status_code}).")
            except Exception as e:
                print(f" -> Error Paket {batch_number}: {e}")
                
            time.sleep(1)
            
        print(f"PROSES SELESAI! Data {total_rows} baris sekarang adalah satu-satunya data di Power BI.")
    else:
        print("GAGAL: Tidak ada data.")

if __name__ == "__main__":
    run_fast_scraper()
