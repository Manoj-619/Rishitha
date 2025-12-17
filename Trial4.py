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
        
        # Add options for better compatibility
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        
        # Initialize service - handle case where chromedriver might be in PATH
        if os.path.exists(self.chrome_driver_path):
            service = Service(self.chrome_driver_path)
            self.driver = webdriver.Chrome(service=service, options=options)
        else:
            # Try to use chromedriver from PATH
            self.driver = webdriver.Chrome(options=options)
        
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
            try:
                self.driver.quit()
            except Exception:
                pass  # Ignore errors if browser is already closed
            finally:
                self.driver = None


# ============================================
#         PROCESS A SINGLE CROP + DISTRICT
# ============================================
def process_crop_for_district(scraper, crop_name, crop_id, district, db_config):
    conn = None
    cur = None
    browser_closed = False
    
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
        except Exception:
            scraper.close_browser()
            browser_closed = True
            return False, f"{crop_name} - No data for {district}"

        # Download CSV
        scraper.click_download_csv()
        scraper.wait_for_download(15)
        scraper.close_browser()
        browser_closed = True

        latest_file = scraper.get_latest_file()
        if not latest_file:
            return False, f"{crop_name} - CSV not downloaded for {district}"

        # Read CSV
        try:
            df = pd.read_csv(latest_file, header=2)
        except Exception as e:
            return False, f"{crop_name} - Error reading CSV for {district}: {e}"

        # Find latest price column
        price_cols = [c for c in df.columns if c.startswith("Price on")]
        if not price_cols:
            return False, f"{crop_name} - No price columns for {district}"

        latest_col = price_cols[0]
        date_str = latest_col.replace("Price on", "").strip()
        
        try:
            pricedate = datetime.datetime.strptime(date_str, "%d %b, %Y").date()
        except Exception as e:
            return False, f"{crop_name} - Error parsing date for {district}: {e}"

        maxprice = df[latest_col].dropna().max()  # take latest date price
        if pd.isna(maxprice):
            return False, f"{crop_name} - No price value for {district}"

        # MSP column if present
        msp_cols = [c for c in df.columns if "MSP" in c]
        modelprice = float(df[msp_cols[0]].max()) if msp_cols and not df[msp_cols[0]].isna().all() else None

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
        cur = None
        conn.close()
        conn = None

        return True, f"{crop_name} - Inserted for {district}"

    except Exception as e:
        return False, f"ERROR {crop_name} {district}: {str(e)}"

    finally:
        # Close database connections if still open
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        
        # Close browser only if not already closed
        if not browser_closed:
            scraper.close_browser()


# ============================================
#               EMAIL SENDER
# ============================================
def send_email(subject, body):
    try:
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
        return True
    except Exception as e:
        print(f"Error sending email: {e}")
        return False


# ============================================
#                   MAIN
# ============================================
if __name__ == "__main__":
    import platform
    import sys
    
    # Get the script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Set download directory based on OS
    if platform.system() == "Windows":
        download_dir = os.path.join(script_dir, "downloads")
        # Try to find chromedriver in common locations
        chrome_driver_path = os.path.join(script_dir, "chromedriver-win64", "chromedriver.exe")
        if not os.path.exists(chrome_driver_path):
            # Try alternative path
            chrome_driver_path = os.path.join(os.path.expanduser("~"), "Downloads", "chromedriver-win64", "chromedriver.exe")
    else:  # macOS or Linux
        download_dir = os.path.join(script_dir, "downloads")
        # For macOS/Linux, try to find chromedriver
        chrome_driver_path = os.path.join(script_dir, "chromedriver")
        if not os.path.exists(chrome_driver_path):
            # Try common locations
            possible_paths = [
                "/usr/local/bin/chromedriver",
                "/opt/homebrew/bin/chromedriver",
                os.path.join(os.path.expanduser("~"), "chromedriver"),
            ]
            for path in possible_paths:
                if os.path.exists(path):
                    chrome_driver_path = path
                    break
            else:
                # If not found, use 'chromedriver' and hope it's in PATH
                chrome_driver_path = "chromedriver"
    
    # Create download directory
    os.makedirs(download_dir, exist_ok=True)
    
    # Check if chromedriver exists (skip check if it's in PATH)
    if chrome_driver_path != "chromedriver" and not os.path.exists(chrome_driver_path):
        print(f"Warning: ChromeDriver not found at {chrome_driver_path}")
        print("Trying to use ChromeDriver from system PATH...")
        chrome_driver_path = "chromedriver"  # Fall back to PATH

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
    # SEND EMAIL SUMMARY
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
    if send_email(subject, body):
        print("Email sent successfully.")
    else:
        print("Failed to send email.")