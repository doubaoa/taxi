# -*-coding:utf-8-*-
import datetime
import pandas as pd
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import time

browser = webdriver.Chrome('E:\chromedriver_win32\chromedriver.exe')
browser.get("http://hodor.jdcloud.com/#/space/list")
browser.find_element_by_css_selector('#username')
browser.find_element_by_css_selector('#username').send_keys('nianaixin')
browser.find_element_by_css_selector('#password')
browser.find_element_by_css_selector('#password').send_keys('Billie962464!')
browser.find_element_by_css_selector('body > div > div > div > div.login_pop_inner.login_withpc > form > div.login_form_row.formsubmit > input').click()
wait = WebDriverWait(browser, 60)
wait.until(EC.presence_of_element_located((By.CLASS_NAME, "detail")))
time.sleep(10)
soup = BeautifulSoup(browser.page_source, "lxml")
memory = soup.select('.detail')
print soup.select('.cell')
wuliji = re.search('物理机',str(soup.select('.cell')))
print len(wuliji)
# for i in range(len(wuliji)):
#
# while re.search('物理机',str(soup.select('.cell')))=='物理机':
#     print soup.a.parent
    # for m in memory:
    #     print m
    #     used = re.search('(\d+.*\d*)GB\s+Used/(\d+)GB',str(m))
    #     used.group(1)
    #     print used.group(2)
