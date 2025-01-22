import logging
import os
import time
import pandas as pd
import pandas_gbq as pd_gbq
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from google.oauth2 import service_account
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class BigQueryUploader:
    def __init__(self, credentials_path, project_id):
        self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
        self.project_id = project_id

    def upload_dataframe_to_bigquery(self, df, dataset_id, table_id):
        try:
            logging.info("Uploading DataFrame to BigQuery table %s.%s...", dataset_id, table_id)
            timestamps = pd.date_range("now", periods=len(df), freq='T')
            df['load_timestamp'] = timestamps
            df['load_timestamp'] = df['load_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            logging.info(df.dtypes)
            pd_gbq.to_gbq(
                df,
                destination_table=f"{dataset_id}.{table_id}",
                project_id=self.project_id,
                credentials=self.credentials,
                if_exists="replace"
            )
            logging.info("DataFrame uploaded successfully.")
        except Exception as e:
            logging.error("Failed to upload DataFrame to BigQuery: %s", e)
            raise


class DataDownloader:
    def __init__(self, download_dir, chromedriver_path):
        self.download_dir = Path(download_dir).resolve()
        self.options = Options()
        self.options.add_argument('--headless')
        prefs = {
            "download.default_directory": str(self.download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        self.options.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Chrome(service=Service(chromedriver_path), options=self.options)

    def download_files(self, urls):
        download_status = {}

        for key, url in urls.items():
            try:
                logging.info("Accessing URL for %s: %s", key, url)
                self.driver.get(url)
                time.sleep(5)

                logging.info("Attempting to locate the download button for %s...", key)
                download_button = self.driver.find_element(By.XPATH, '/html/body/div[1]/div[3]/div[3]/div[1]/div[1]/div[5]/a')
                download_button.click()
                time.sleep(5)

                download_status[key] = {"success": True, "pattern": key}
                logging.info("Download completed for %s.", key)
            except Exception as e:
                logging.error("Error downloading %s: %s", key, e)
                download_status[key] = {"success": False, "pattern": key}

        return download_status

    def close_driver(self):
        try:
            self.driver.quit()
            logging.info("WebDriver closed successfully.")
        except Exception as e:
            logging.error("Error closing WebDriver: %s", e)


def rename_files(download_folder, file_info):
    files = os.listdir(download_folder)
    for key, details in file_info.items():
        if details["success"]:
            pattern = details["pattern"]
            for file in files:
                if pattern in file and file.endswith('.xlsx'):
                    old_path = os.path.join(download_folder, file)
                    new_name = f"{key}.xlsx"
                    new_path = os.path.join(download_folder, new_name)
                    os.rename(old_path, new_path)
                    logging.info("File renamed: %s -> %s", file, new_name)
                    break
            else:
                logging.warning("File matching pattern '%s' not found for renaming.", pattern)


def main():
    download_dir = Path(__file__).parent.resolve()
    chromedriver_path = r"C:\Users\anafl\Documents\ds3x_\chromedriver-win64\chromedriver.exe"
    credentials_path = f"{str(download_dir)}/credentials.json"
    project_id = "ps-eng-dados-ds3x"
    dataset_id = "anaflavia_199"
    urls = {
        "icc": "https://www.fecomercio.com.br/pesquisas/indice/icc",
        "icf": "https://www.fecomercio.com.br/pesquisas/indice/icf"
    }

    downloader = DataDownloader(download_dir, chromedriver_path)
    uploader = BigQueryUploader(credentials_path, project_id)

    try:
        download_status = downloader.download_files(urls)
    finally:
        downloader.close_driver()

    rename_files(download_dir, download_status)

    sheet = {
        "icc": "SÉRIE",
        "icf": "Série Histórica"
    }

    for key, status in download_status.items():
        if status["success"]:
            file_path = download_dir / f"{key}.xlsx"
            #sheet_name = sheet[key]
            table_id = f"{key}_raw"
            
            df = pd.read_excel(file_path, skiprows=1)
            df = df.dropna(how='all')
            df = df.drop(df.index[-1])
            del df['Unnamed: 1']
            df.rename(columns={"Unnamed: 0": "ÍNDICES E SEGMENTAÇÕES"}, inplace=True)
            df.columns = [col.strftime('%m/%Y') if isinstance(col, datetime) else col for col in df.columns]
            df = df.dropna(axis=1, how='all')
            df.columns = [col.replace('/', '_') if isinstance(col, str) else col for col in df.columns]
            print(df)
            uploader.upload_dataframe_to_bigquery(df, dataset_id, table_id)


if __name__ == "__main__":
    main()