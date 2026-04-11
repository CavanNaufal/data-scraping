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
    
    # Official Indonesian Province Codes (38 Provinces)
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
        print("Warming up the browser to bypass Cloudflare...")
        driver.get('https://keslan.kemkes.go.id/app/siranap/')
        time.sleep(10)
        print("Warm-up complete. Starting fast data extraction...")

        for i in province_codes:
            prop_code = f"{i}prop"
            url = f'https://keslan.kemkes.go.id/app/siranap/rumah_sakit?jenis=2&propinsi={prop_code}&kabkota='
            print(f"Checking province code: {prop_code}...")
            
            driver.get(url)
            
            try:
                WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "cardRS"))
                )
                
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
                                    'Available Beds': just_number,
                                    'Last Update': columns[3].text.strip()
                                })
            except TimeoutException:
                print(f" -> Skipped: No data found for {prop_code}")
                pass 
            
    finally:
        driver.quit()
        
    if all_data:
        df = pd.DataFrame(all_data)
        df['Available Beds'] = pd.to_numeric(df['Available Beds'])
        
        file_name = 'siranap_data.csv'
        df.to_csv(file_name, index=False)
        print(f"SUCCESS! {len(df)} rows saved to CSV.")
    else:
        print("FAILED: No data was extracted.")

if __name__ == "__main__":
    run_fast_scraper()
