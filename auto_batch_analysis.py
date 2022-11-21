import os
import datetime
import gspread
from time import sleep
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By
from fake_useragent import UserAgent
from oauth2client.service_account import ServiceAccountCredentials
from webdriver_manager.chrome import ChromeDriverManager

# Logger setting
from logging import getLogger, FileHandler, DEBUG
logger = getLogger(__name__)
today = datetime.datetime.now()
os.makedirs('./log', exist_ok=True)
handler = FileHandler(f'log/{today.strftime("%Y-%m-%d")}_result.log', mode='a')
handler.setLevel(DEBUG)
logger.setLevel(DEBUG)
logger.addHandler(handler)
logger.propagate = False

### functions ###
def get_domain_info(sheet):
    SPREADSHEET_ID = os.environ['DOMAIN_LIST_SSID']
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('spreadsheet.json', scope)
    gc = gspread.authorize(credentials)
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(sheet)

    cell_list = ws.col_values(2)
    cell_list.pop(0)
    logger.debug(f'main: {sheet} Size: {len(cell_list)}')
    return cell_list

def split_list(l, n):
    for index in range(0, len(l), n):
        yield l[index:index + n]

def extract_text(elements):
    for element in elements:
        yield element.text

def batch_analysis(domain_info):
    url_login = 'https://app.ahrefs.com/user/login'
    url_ba = 'https://app.ahrefs.com/batch-analysis'
    login = os.environ['AHREFS_ID']
    password = os.environ['AHREFS_PASS']

    ua = UserAgent()
    logger.debug(f'main: UserAgent: {ua.chrome}')

    options = Options()
#   options.add_argument('--headless')
    options.add_argument(f'user-agent={ua.chrome}')

    try:
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

        driver.get(url_login)
        driver.maximize_window()
        driver.implicitly_wait(60)

        driver.find_element(By.NAME, 'email').send_keys(login)
        driver.find_element(By.NAME, 'password').send_keys(password)
        driver.find_element(By.XPATH, '//input[@type="checkbox"]').click()
        sleep(1)
        driver.find_element(By.XPATH, '//button[@type="submit"]').click()
        logger.info(f'batch_analysis: Sign in')
        driver.implicitly_wait(60)

        driver.find_element(By.XPATH, '//a[@href="/batch-analysis"]')
        driver.get(url_ba)
        sleep(5)

        domain_list = list(split_list(domain_info, 200))
        logger.debug(f'domain_list: size: {len(domain_list)}')
        drl = []
        ipl = []
        gov = []
        edu = []
        for domain_chunk in domain_list:
            driver.implicitly_wait(60)
            search = driver.find_element(By.ID, 'batch_requests')
            search.send_keys("\n".join(domain_chunk))
            driver.find_element(By.ID, 'startAnalysisButton').click()
            driver.implicitly_wait(600)
            driver.find_element(By.ID, 'batch_data_table')
            sleep(3)

            drl_dr = driver.find_elements(By.NAME, 'domain_rating')
            sleep(1)
            ipl_dr = driver.find_elements(By.XPATH, '//*[@id="batch_data_table"]/tbody/tr/td[15]')
            sleep(1)
            gov_dr = driver.find_elements(By.XPATH, '//*[@id="batch_data_table"]/tbody/tr/td[12]')
            sleep(1)
            edu_dr = driver.find_elements(By.XPATH, '//*[@id="batch_data_table"]/tbody/tr/td[13]')
            sleep(1)

            drl.extend(list(extract_text(drl_dr)))
            ipl.extend(list(extract_text(ipl_dr)))
            gov.extend(list(extract_text(gov_dr)))
            edu.extend(list(extract_text(edu_dr)))
            sleep(2)
            driver.get(url_ba)

        logger.debug(f'Size: dnl: {len(domain_info)}')
        logger.debug(f'Size: drl: {len(drl)}')
        logger.debug(f'Size: ipl: {len(ipl)}')
        logger.debug(f'Size: gov: {len(gov)}')
        logger.debug(f'Size: edu: {len(edu)}')
        result = []
        for dn, dr, ip, go, ed in zip(domain_info, drl, ipl, gov, edu):
            result.append([dn, dr, ip, go, ed])

        driver.close()
        driver.quit()

        return result
    except Exception as err:
        logger.error(f'batch_analysis: {err}')

def write_batch_info(sheet, data):
    SPREADSHEET_ID = os.environ['DOMAIN_LIST_SSID']
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('spreadsheet.json', scope)
    gc = gspread.authorize(credentials)
    sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(sheet)

    cell_list = sheet.range('B2:F' + str(len(data) + 1))
    for index, cell in enumerate(cell_list):
        cell.value = data[int(index / 5)][int(index % 5)]

    sheet.update_cells(cell_list, value_input_option='USER_ENTERED')

### main_script ###
if __name__ == '__main__':
    try:
        domain_info = get_domain_info('List-Japanese')
        batch_info = batch_analysis(domain_info)
        write_batch_info('List-Japanese', batch_info)
        domain_info = get_domain_info('List-NotJapanese')
        batch_info = batch_analysis(domain_info)
        write_batch_info('List-NotJapanese', batch_info)
        logger.info('Finish')
        exit(0)
    except Exception as err:
        logger.error(f'main: {err}')
        exit(1)
