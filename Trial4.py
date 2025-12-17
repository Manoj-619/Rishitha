import os
import time
import datetime
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

import psycopg2
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


# ============================================
#           SELENIUM SCRAPER CLASS
# ============================================
class AgmarknetScraper:
    def __init__(self, download_dir, chrome_driver_path):
        self.download_dir = download_dir
        self.chrome_driver_path = chrome_driver_path
        self.driver = None
        self.wait = None

    def initialize_driver(self):
        options = webdriver.ChromeOptions()
        prefs = {"download.default_directory": self.download_dir}
        options.add_experimental_option("prefs", prefs)
        service = Service(self.chrome_driver_path)
        self.driver = webdriver.Chrome(service=service, options=options)
        self.driver.maximize_window()
        self.wait = WebDriverWait(self.driver, 20)

    def open_website(self, url):
        self.driver.get(url)

    # ----------------- SELECT STATE + DISTRICT -----------------
    def select_state_and_district(self, district_name):
        # STATE
        state_dd = self.wait.until(EC.element_to_be_clickable((By.ID, "state")))
        state_dd.click()
        time.sleep(1)

        # Uncheck All States
        all_states = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//span[text()='All States']/parent::div"))
        )
        all_states.click()
        time.sleep(1)

        # Select Karnataka
        state_karnataka = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//span[text()='Karnataka']/parent::div"))
        )
        state_karnataka.click()
        time.sleep(1)

        ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
        time.sleep(1)

        # DISTRICT
        district_dd = self.wait.until(EC.element_to_be_clickable((By.ID, "district")))
        district_dd.click()
        time.sleep(1)

        # Uncheck All Districts
        all_districts = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//span[contains(text(),'All District')]/parent::div"))
        )
        all_districts.click()
        time.sleep(1)

        # Select district (Bidar / Raichur)
        dist_el = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, f"//span[text()='{district_name}']/parent::div"))
        )
        dist_el.click()
        time.sleep(1)

        ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
        time.sleep(1)

    # ----------------- SELECT COMMODITY -----------------
    def select_commodity(self, commodity_name):
        commodity_dd = self.wait.until(EC.element_to_be_clickable((By.ID, "commodity")))
        commodity_dd.click()
        time.sleep(1)

        option = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, f"//span[normalize-space()='{commodity_name}']/parent::div"))
        )
        option.click()
        time.sleep(1)

        ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
        time.sleep(1)

    def click_go(self):
        go_btn = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Go')]"))
        )
        go_btn.click()

    def click_download_csv(self):
        download_btn = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(@aria-label,'Download')]"))
        )
        download_btn.click()
        time.sleep(1)

        csv_btn = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Download as CSV')]"))
        )
        csv_btn.click()

    def wait_for_download(self, seconds=20):
        time.sleep(seconds)

    def get_latest_file(self):
        files = os.listdir(self.download_dir)
        csvs = [os.path.join(self.download_dir, f) for f in files if f.endswith(".csv")]
        return max(csvs, key=os.path.getmtime) if csvs else None

    def close_browser(self):
        time.sleep(1)
        if self.driver:
            self.driver.quit()


# ============================================
#         PROCESS A SINGLE CROP + DISTRICT
# ============================================
def process_crop_for_district(scraper, crop_name, crop_id, district, db_config):

    try:
        scraper.initialize_driver()
        scraper.open_website("https://www.agmarknet.gov.in/home")
        time.sleep(3)

        scraper.select_state_and_district(district)
        scraper.select_commodity(crop_name)

        scraper.click_go()
        time.sleep(4)

        # Check if table exists (if no data → timeout)
        try:
            scraper.wait.until(
                EC.presence_of_element_located((By.XPATH, "//table"))
            )
        except:
            scraper.close_browser()
            return False, f"{crop_name} - No data for {district}"

        # Download CSV
        scraper.click_download_csv()
        scraper.wait_for_download(15)
        scraper.close_browser()

        latest_file = scraper.get_latest_file()
        if not latest_file:
            return False, f"{crop_name} - CSV not downloaded for {district}"

        # Read CSV
        df = pd.read_csv(latest_file, header=2)

        # Find latest price column
        price_cols = [c for c in df.columns if c.startswith("Price on")]
        if not price_cols:
            return False, f"{crop_name} - No price columns for {district}"

        latest_col = price_cols[0]
        date_str = latest_col.replace("Price on", "").strip()
        pricedate = datetime.datetime.strptime(date_str, "%d %b, %Y").date()

        maxprice = df[latest_col].dropna().max()  # take latest date price
        if pd.isna(maxprice):
            return False, f"{crop_name} - No price value for {district}"

        # MSP column if present
        msp_cols = [c for c in df.columns if "MSP" in c]
        modelprice = df[msp_cols[0]].max() if msp_cols else None

        # Insert into DB
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO public.tb_mst_crop_price
            (district, market, crop_name, variety_name, grade,
             minprice, maxprice, modelprice, pricedate, crop_id, state)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                district,
                "",                     # market empty
                crop_name,
                "",                     # variety empty
                "",                     # grade empty
                None,                   # minprice = NULL
                float(maxprice),        # max price (latest date)
                modelprice,             # modelprice = MSP
                pricedate,
                crop_id,
                "Karnataka"
            )
        )

        conn.commit()
        cur.close()
        conn.close()

        return True, f"{crop_name} - Inserted for {district}"

    except Exception as e:
        return False, f"ERROR {crop_name} {district}: {e}"

    finally:
        scraper.close_browser()


# ============================================
#               EMAIL SENDER
# ============================================
def send_email(subject, body):
    smtp_server = "smtp.office365.com"
    smtp_port = 587
    sender_email = "job.notification@plentifarms.com"
    sender_password = "Pl3nt1f0rM"

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join([
        "Rishitha.Akbote@plentifarms.com"
    ])
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)


# ============================================
#                   MAIN
# ============================================
if __name__ == "__main__":


    download_dir = r"C:\Users\RishithaAkbote\OneDrive - Plentifarms\Documents\Task13_RPA_Crop_price\downloads"
    chrome_driver_path = r"C:\Users\RishithaAkbote\OneDrive - Plentifarms\Documents\Task13_RPA_Crop_price\chromedriver-win64\chromedriver.exe"

    os.makedirs(download_dir, exist_ok=True)

    db_config = {
        "database": "pfmarketplace",
        "user": "pfmarket",
        "password": "P6*n%I-mZTqNs_H",
        "host": "strapidb-dev.cbz0pmz2spl7.ap-south-1.rds.amazonaws.com",
        "port": "5432"
    }

    crop_list = [
        {"name": "Paddy(Common)", "id": 8},
        {"name": "Arhar(Tur/Red Gram)(Whole)", "id": 10},
        {"name": "Bengal Gram(Gram)(Whole)", "id": 1},
        {"name": "Bajra(Pearl Millet/Cumbu)", "id": 12},
        {"name": "Groundnut", "id": 4},
        {"name": "Sunflower", "id": 9},
        {"name": "Green Gram(Moong)(Whole)", "id": 6},
        {"name": "Black Gram(Urd Beans)(Whole)", "id": 11},
    ]

    email_logs = []
    data_saved = False




    for crop in crop_list:

        # Run for two districts separately
        for district in ["Bidar", "Raichur"]:

            scraper = AgmarknetScraper(download_dir, chrome_driver_path)

            result, message = process_crop_for_district(
                scraper,
                crop["name"],
                crop["id"],
                district,
                db_config
            )

            # Collect logs
            print(message)
            email_logs.append(message)

            # Mark if ANY district saved data
            if result:
                data_saved = True

    # ---------------------------------------
    # SEND EMAIL SUMMARY (EXACTLY LIKE OLD CODE)
    # ---------------------------------------
    subject = (
        "Crop Price RPA - Data Saved in DB"
        if data_saved
        else "Crop Price RPA - No Data Found for Specified Districts"
    )

    body = (
        "Crop Price RPA from Agmarknet completed.\n\n"
        "Execution Summary:\n\n" + "\n".join(email_logs)
    )

    print("Sending email summary...")
    send_email(subject, body)
    print("Email sent successfully.")

    #  # Send an email if data was saved successfully
    if data_saved:
        # Specify the email subject and body
        subject = "Data Saved in DB tb_mst_crop_price"
        body = "Crop Price RPA from Agmarknet:\n\n" + "\n".join(email_logs)
        # Send the email
        send_email(subject, body)