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
import os

def jalankan_scraper():
    print("Menyiapkan browser headless di server GitHub...")
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    # Menyamar sebagai manusia agar Cloudflare lebih ramah
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    driver = webdriver.Chrome(options=chrome_options)
    semua_data = []
    
    try:
        for i in range(1, 101):
            kode_prop = f"{i}prop"
            url = f'https://keslan.kemkes.go.id/app/siranap/rumah_sakit?jenis=2&propinsi={kode_prop}&kabkota='
            print(f"Mengecek provinsi: {kode_prop}...")
            
            driver.get(url)
            
            try:
                # Menunggu 10 detik, memberi waktu Cloudflare loading jika ada
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "cardRS"))
                )
                time.sleep(2)
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                # Ekstrak Nama Provinsi
                nama_propinsi = f"Kode {kode_prop}"
                opsi_prop = soup.find('option', value=kode_prop)
                if opsi_prop:
                    nama_propinsi = opsi_prop.text.strip()
                
                cards = soup.find_all('div', class_='cardRS')
                
                for card in cards:
                    nama_rs = card.find('h5').text.strip() if card.find('h5') else "-"
                    tabel = card.find('table')
                    
                    if tabel:
                        for tr in tabel.find_all('tr'):
                            kolom = tr.find_all('td')
                            if len(kolom) == 4:
                                # Ekstrak hanya angka untuk ketersediaan bed
                                teks_ketersediaan = kolom[2].text.strip()
                                match_angka = re.search(r'\d+', teks_ketersediaan)
                                angka_saja = match_angka.group() if match_angka else "0"
                                
                                semua_data.append({
                                    'Provinsi': nama_propinsi,
                                    'Nama RS': nama_rs,
                                    'Kelas': kolom[0].text.strip(),
                                    'Ruangan': kolom[1].text.strip(),
                                    'Tersedia': angka_saja,
                                    'Update': kolom[3].text.strip()
                                })
            except TimeoutException:
                pass # Lewati jika kosong/diblokir keras
            
            time.sleep(1)
            
    finally:
        driver.quit()
        
    if semua_data:
        df = pd.DataFrame(semua_data)
        df['Tersedia'] = pd.to_numeric(df['Tersedia']) # Format ke angka
        file_name = 'data_siranap.csv'
        df.to_csv(file_name, index=False)
        print(f"BERHASIL! {len(df)} baris data disimpan.")
    else:
        print("GAGAL: Tidak ada data yang berhasil ditarik.")

if __name__ == "__main__":
    jalankan_scraper()
