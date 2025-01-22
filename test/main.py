import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pathlib import Path
from main import BigQueryUploader, DataDownloader, rename_files

class TestBigQueryUploader(unittest.TestCase):
    @patch('main.pd_gbq.to_gbq')
    @patch('main.service_account.Credentials.from_service_account_file')
    def test_upload_dataframe_to_bigquery(self, mock_credentials, mock_to_gbq):
        mock_credentials.return_value = MagicMock()
        uploader = BigQueryUploader('fake_credentials_path', 'fake_project_id')
        
        df = pd.DataFrame({
            'col1': [1, 2],
            'col2': [3, 4],
            'Mês': pd.to_datetime(['2021-01-01', '2021-02-01'])
        })
        
        uploader.upload_dataframe_to_bigquery(df, 'fake_dataset_id', 'fake_table_id')
        
        self.assertTrue(mock_to_gbq.called)
        self.assertEqual(mock_to_gbq.call_args[1]['destination_table'], 'fake_dataset_id.fake_table_id')

class TestDataDownloader(unittest.TestCase):
    @patch('main.webdriver.Chrome')
    def test_download_files(self, mock_chrome):
        mock_driver = MagicMock()
        mock_chrome.return_value = mock_driver
        
        downloader = DataDownloader('fake_download_dir', 'fake_chromedriver_path')
        urls = {'test': 'http://fakeurl.com'}
        
        mock_driver.find_element.return_value.click = MagicMock()
        
        download_status = downloader.download_files(urls)
        
        self.assertIn('test', download_status)
        self.assertTrue(download_status['test']['success'])
        
        downloader.close_driver()
        self.assertTrue(mock_driver.quit.called)

class TestRenameFiles(unittest.TestCase):
    @patch('main.os.listdir')
    @patch('main.os.rename')
    def test_rename_files(self, mock_rename, mock_listdir):
        mock_listdir.return_value = ['test_pattern.xlsx']
        download_folder = 'fake_download_folder'
        file_info = {'test': {'success': True, 'pattern': 'test_pattern'}}
        
        rename_files(download_folder, file_info)
        
        mock_rename.assert_called_once_with(
            'fake_download_folder/test_pattern.xlsx',
            'fake_download_folder/test.xlsx'
        )

if __name__ == '__main__':
    unittest.main()