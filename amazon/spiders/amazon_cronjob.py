import os
import time
import json
import scrapy
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class AmazoncronjobSpider(scrapy.Spider):
    name = "amazon_cronjob"
    base_url = 'https://www.amazon.com'

    load_dotenv(dotenv_path='./info.env')
    DB_HOST = os.getenv('DB_HOST')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')

    db = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

    def start_requests(self):
        cursor = self.cursor
        cursor.execute("SELECT * FROM scrapping_urls WHERE retailer_id = 1 AND status = 'active';")
        link_values = [row[0] for row in cursor.fetchall()]
        
        for link in link_values:
            yield SeleniumRequest(url=link, callback=self.check_product)

    def check_product(self, response):
        discount_percentage = response.xpath('//span[contains(text(), "List Price:")]//span[@class="a-offscreen"]/ancestor::div//span[@class="a-size-large a-color-price savingPriceOverride aok-align-center reinventPriceSavingsPercentageMargin savingsPercentage"]/text()').get()

        if discount_percentage:
            print("\n", 'The product is on discount')
            print("***************************************")
            print("Price: ", discount_percentage, "\n")
            print("************************************************")
        else:
            print('The product is not on discount.\n')

            product_url = response.url
            cursor = self.cursor

            # Check if the product exists in the database
            cursor.execute("SELECT * FROM product WHERE url = %s", (product_url,))
            matching_document = cursor.fetchone()

            if matching_document:
                print("Matching Document: ")
                print(matching_document)

                # Remove the matching document from the table
                cursor.execute("DELETE FROM product WHERE url = %s", (product_url,))
                self.db.commit()

                print('Deleted one product: ' + str(product_url))
