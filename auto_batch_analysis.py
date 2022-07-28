import os
import datetime
import gspread
from time import sleep
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.select import Select
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

        driver.find_element_by_name('email').send_keys(login)
        driver.find_element_by_name('password').send_keys(password)
        driver.find_element_by_xpath('//button[@type="submit"]').click()
        logger.info(f'batch_analysis: Sign in')
        driver.implicitly_wait(60)

        driver.find_element_by_xpath('//button[@data-toggle="dropdown"]').click()
        driver.implicitly_wait(20)
        driver.find_element_by_xpath('//a[@href="/batch-analysis"]').click()
        sleep(5)

        domain_list = list(split_list(domain_info, 200))
        dr_list = list()
        ip_list = list()
        for domain_chunk in domain_list:
            search = driver.find_element_by_id('batch_requests')
            search.send_keys("\n".join(domain_chunk))
            driver.find_element_by_id('startAnalysisButton').click()
            driver.implicitly_wait(60)
            dr = driver.find_elements_by_name('domain_rating')
            dr_list.extend(list(extract_text(dr)))
            ip = driver.find_elements_by_xpath('//tr/td[15]')
            ip_list.extend(list(extract_text(ip)))
            driver.get(url_ba)

        driver.close()
        driver.quit()

        return [dr_list, ip_list]
    except Exception as err:
        logger.error(f'batch_analysis: {err}')

def write_batch_info(sheet, dr, ip):
    SPREADSHEET_ID = os.environ['DOMAIN_LIST_SSID']
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('spreadsheet.json', scope)
    gc = gspread.authorize(credentials)
    sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(sheet)

    cell_list = sheet.range('C2:C' + str(len(dr) + 1))
    i = 0
    for cell in cell_list:
        cell.value = dr[i]
        i += 1
    sheet.update_cells(cell_list, value_input_option='USER_ENTERED')

    cell_list = sheet.range('D2:D' + str(len(ip) + 1))
    i = 0
    for cell in cell_list:
        cell.value = ip[i]
        i += 1
    sheet.update_cells(cell_list, value_input_option='USER_ENTERED')

### main_script ###
if __name__ == '__main__':
    try:
        domain_info = get_domain_info('List-Japanese')
        batch_info = batch_analysis(domain_info)
        write_batch_info('List-Japanese', batch_info[0], batch_info[1])
        domain_info = get_domain_info('List-NotJapanese')
        batch_info = batch_analysis(domain_info)
        write_batch_info('List-NotJapanese', batch_info[0], batch_info[1])
        logger.info('Finish')
        exit(0)
    except Exception as err:
        logger.error(f'main: {err}')
        exit(1)
