from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException
import unittest, time, re

true=1
false=0

class Export(unittest.TestCase):
    def setUp(self):
        self.driver = webdriver.Firefox()
        self.driver.implicitly_wait(30)
        self.base_url = "http://www.billiger.de/"
        self.verificationErrors = []
        self.accept_next_alert = true

    def test_export(self):
        driver = self.driver
        driver.get(self.base_url + "/")
        driver.find_element_by_link_text("Herrenschuhe").click()
        driver.find_element_by_css_selector("div.filterbox_v2_group.header").click()
        driver.find_element_by_xpath("//div[@id='filterbox_v2']/div/div/div[6]/p[2]/img[2]").click()
        driver.find_element_by_xpath("//div[@id='filterbox_v2']/div[2]/div/div[2]/p[2]/img[2]").click()
        driver.find_element_by_xpath("//div[@id='filterbox_v2']/div[2]/div/div[3]/ul/li/span").click()

    def is_element_present(self, how, what):
        try: self.driver.find_element(by=how, value=what)
        except NoSuchElementException, e: return False
        return True

    def close_alert_and_get_its_text(self):
        try:
            alert = self.driver.switch_to_alert()
            if self.accept_next_alert:
                alert.accept()
            else:
                alert.dismiss()
            return alert.text
        finally: self.accept_next_alert = True

    def tearDown(self):
        self.driver.quit()
        self.assertEqual([], self.verificationErrors)

if __name__ == "__main__":
    unittest.main()
