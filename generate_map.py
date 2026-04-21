import os
import json
import pandas as pd
import folium
from google.oauth2 import service_account
from google.cloud import bigquery

# 1. Autentikasi ke Google BigQuery
gcp_json_str = os.environ.get("GCP_CREDENTIALS")
if not gcp_json_str:
    print("Error: GCP_CREDENTIALS tidak ditemukan.")
    exit()

gcp_info = json.loads(gcp_json_str)
credentials = service_account.Credentials.from_service_account_info(gcp_info)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# 2. Ambil Data Tarikan Terakhir (FOKUS: Occupied Beds)
# Kita membuang BOR dan mengambil Total_Beds, Occupied_Beds, dan Available_Beds
query = f"""
    WITH LatestTime AS (
        SELECT MAX(Sent_Date) as max_time FROM `{credentials.project_id}.siranap_db.bed_capacity`
    )
    SELECT 
        Province, 
        SUM(Total_Beds) as Total_Capacity,
        SUM(Occupied_Beds) as Total_Occupied,
        SUM(Available_Beds) as Total_Available
    FROM `{credentials.project_id}.siranap_db.bed_capacity`
    WHERE Sent_Date = (SELECT max_time FROM LatestTime)
    GROUP BY Province
"""
print("Menarik data terbaru dari BigQuery...")
df = client.query(query).to_dataframe()

# 3. Kamus Koordinat Provinsi (Sesuai persis dengan data Kemenkes)
prov_coords = {
    "ACEH": [4.6951, 96.7493], 
    "BALI": [-8.4095, 115.1889],
    "BANTEN": [-6.4058, 106.0640], 
    "BENGKULU": [-3.5778, 102.3464],
    "D I YOGYAKARTA": [-7.7956, 110.3695], 
    "DKI JAKARTA": [-6.2088, 106.8456],
    "GORONTALO": [0.5435, 122.2987], 
    "JAMBI": [-1.6101, 103.6131],
    "JAWA BARAT": [-6.9204, 107.6022], 
    "JAWA TENGAH": [-7.1509, 110.1402],
    "JAWA TIMUR": [-7.2504, 112.7688], 
    "KALIMANTAN BARAT": [-0.2298, 111.1140],
    "KALIMANTAN SELATAN": [-3.0926, 115.2837], 
    "KALIMANTAN TENGAH": [-1.6814, 113.3823],
    "KALIMANTAN TIMUR": [0.5386, 116.4193], 
    "KALIMANTAN UTARA": [3.0730, 116.0413],
    "KEPULAUAN BANGKA BELITUNG": [-2.7410, 106.4405], 
    "KEPULAUAN RIAU": [3.9456, 108.1429],
    "LAMPUNG": [-4.5585, 105.1065], 
    "MALUKU": [-3.2384, 130.1452],
    "MALUKU UTARA": [1.5709, 127.8087], 
    "NUSA TENGGARA BARAT": [-8.6529, 117.3616],
    "NUSA TENGGARA TIMUR": [-8.6573, 121.0793], 
    "PAPUA": [-4.2699, 138.0803],
    "PAPUA BARAT": [-1.3361, 133.1747], 
    "R I A U": [0.2933, 101.7068],
    "SULAWESI BARAT": [-2.8441, 119.2320], 
    "SULAWESI SELATAN": [-4.1449, 119.9065],
    "SULAWESI TENGAH": [-1.4300, 121.4456], 
    "SULAWESI TENGGARA": [-4.1449, 122.1746],
    "SULAWESI UTARA": [0.9614, 124.8632], 
    "SUMATERA BARAT": [-0.7399, 100.8000],
    "SUMATERA SELATAN": [-3.3194, 104.9147], 
    "SUMATERA UTARA": [2.1153, 99.5451]
}

# 4. Membuat Peta Leaflet (Titik Tengah Indonesia)
m = folium.Map(location=[-0.7892, 113.9213], zoom_start=5, tiles="CartoDB positron")

# --- Legenda Peta (Berdasarkan Volume Kasur Terisi) ---
legenda_html = '''
     <div style="position: fixed; 
     bottom: 50px; left: 50px; width: 180px; height: 130px; 
     border:2px solid grey; z-index:9999; font-size:14px;
     background-color:white; opacity: 0.9; padding: 10px;">
     <b>Volume Kasur Terisi</b><br>
     <i style="background:red;width:18px;height:18px;float:left;margin-right:8px;border-radius:50%"></i> Tinggi (> 2.000)<br>
     <i style="background:orange;width:18px;height:18px;float:left;margin-right:8px;border-radius:50%"></i> Sedang (500 - 2.000)<br>
     <i style="background:green;width:18px;height:18px;float:left;margin-right:8px;border-radius:50%"></i> Rendah (< 500)<br>
     </div>
     '''
m.get_root().html.add_child(folium.Element(legenda_html))

# 5. Memasukkan Data ke Peta
print("Membangun peta Leaflet (Fokus Occupied Beds)...")
for index, row in df.iterrows():
    prov = row['Province']
    
    if prov in prov_coords:
        lat, lon = prov_coords[prov]
        occupied = row['Total_Occupied']
        
        # A. Logika Warna berdasarkan Volume Kasur Terisi (Bukan Persentase)
        # Angka ini bisa Anda sesuaikan dengan standar manajemen
        if occupied > 2000:
            color = "red"
        elif occupied >= 500:
            color = "orange"
        else:
            color = "green"
            
        # B. Radius dinamis berdasarkan Volume Kasur Terisi
        # Rumus: 5 (ukuran dasar) + (jumlah pasien / pembagi). 
        # Pembagi 200 agar provinsi dengan 10.000 pasien tidak menutupi seluruh peta.
        radius_size = 5 + (occupied / 200)
        
        # Teks Pop-up HTML (Menonjolkan Kasur Terisi)
        popup_html = f"""
        <div style="width:230px; font-family:Arial, sans-serif;">
            <h4 style="margin-bottom:5px;">{prov}</h4>
            <hr style="margin:5px 0;">
            <b style="font-size:1.3em; color:{color};">Kasur Terisi: {int(occupied):,}</b><br>
            <span style="font-size:0.9em; color:grey;">Kasur Kosong: {int(row['Total_Available']):,}</span><br>
            <span style="font-size:0.9em; color:grey;">Total Kapasitas: {int(row['Total_Capacity']):,}</span>
        </div>
        """
        
        # Tambahkan penanda ke peta
        folium.CircleMarker(
            location=[lat, lon],
            radius=radius_size,
            popup=folium.Popup(popup_html, max_width=350),
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.6,
            weight=2
        ).add_to(m)
    else:
        print(f"Peringatan: Koordinat untuk {prov} tidak ditemukan.")

# 6. Simpan sebagai HTML
output_file = "hospital_volume_map.html"
m.save(output_file)
print(f"✅ Peta berhasil dibuat! Silakan buka file '{output_file}'.")
