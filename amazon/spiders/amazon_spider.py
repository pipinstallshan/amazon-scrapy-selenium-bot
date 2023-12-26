import os
import re
import time
import json
import scrapy
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
from urllib.parse import urlparse
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class AmazonSpider(scrapy.Spider):
    name = "amazon_spi"
    base_url = 'https://www.amazon.com'
    
    _id = 0
    status = ""
    url = ""
    user_id = 0
    country_id = 0
    retailer_id = 0
    scrapping_urls_cat = ""
    
    def __init__(self):
        load_dotenv(dotenv_path='info.env')
        DB_HOST = os.getenv('DB_HOST')
        DB_NAME = os.getenv('DB_NAME')
        DB_USER = os.getenv('DB_USER')
        DB_PASSWORD = os.getenv('DB_PASSWORD')

        self.db = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=3306,
            use_pure=False
        )
        self.cursor = self.db.cursor()
        
    def start_requests(self):
        query = "SELECT * FROM scrapping_urls WHERE retailer_id = 1 AND status = 'active';"
        cursor = self.db.cursor()
        cursor.execute(query)
        for (_id , user_id, country_id, url, status, retailer_id) in cursor.fetchall():
            # The url that it fetches from the db to initialize the crawl process:
            # https://www.amazon.com/s?k=bags+for+women&i=fashion-womens-handbags&rh=n%3A15743631%2Cp_n_material_browse%3A17037742011%2Cp_n_deal_type%3A23566065011&dc&crid=FK51NQ8M44U&qid=1703572929&rnid=23566063011&sprefix=bags%2Caps%2C580&ref=sr_nr_p_n_deal_type_1&ds=v1%3AVH09CG57kzepqvM7I9jBlq0S5SlEVSQhRVNtJgku04k
            
            query_cat = f"SELECT category_id FROM scrapping_urls_category WHERE scrapping_urls_id = {str(_id)}"
            temp = self.db.cursor()
            temp.execute(query_cat)
            con_int = temp.fetchone()
            self.scrapping_urls_cat = int(con_int[0])
           
            if self.scrapping_urls_cat:
                self._id = _id
                self.url = url
                self.user_id = user_id
                self.country_id = country_id
                self.status = status
                self.retailer_id = retailer_id
            
                if 'www.amazon.com/store' in url:
                    yield SeleniumRequest(url=url, callback=self.parse_store, wait_time=5, dont_filter=True)
                else:
                    yield SeleniumRequest(url=url, callback=self.parse_category, wait_time=5, dont_filter=True)
            else:
                print("\n\n\nQUERY ERROR!\n\n\n")
                continue
            
    def parse_store(self, response):
        products = []
        products1 = response.xpath('//ul[@class="ProductGrid__grid__f5oba"]/li[@data-testid="product-grid-item"]//a[@data-testid="grid-item-image"]/@href').getall()
        for i in products1:
            products.append(i)
            
        products2 = response.xpath('//a[@class="Overlay__overlay__LloCU EditorialTile__overlay__RMD1L"]/@href').getall()
        for j in products2:
            products.append(j)

        products3 = response.xpath('//ul[@class="Navigation__navList__HrEra"]/li/a/@href').getall()
        for c in products3:
            products.append(c)

        if products != [] and products != None:
            for product in products:
                yield SeleniumRequest(url=self.base_url + product, callback=self.parse_decide)

    def parse_decide(self, response):
        brand = response.xpath('//a[@id="bylineInfo"]/text()').get()
        if brand != None and brand != '':
            yield SeleniumRequest(url = response.url , callback=self.parse_product)
        else:
            yield SeleniumRequest(url=response.url , callback=self.parse_inner_stores)

    def parse_inner_stores(self , response):
        prods = []
        products = response.xpath('//div[@data-testid="go-to-detail-page"]/a/@href').getall()
        for i in products:
            prods.append(i)
        products2 = response.xpath('//ul[@class="ProductGrid__grid__f5oba"]/li//a[@data-testid="grid-item-image"]/@href').getall()
        for j in products2:
            prods.append(j)
        for i in prods:
            yield SeleniumRequest(url=self.base_url + i , callback=self.parse_product)

    def parse_category(self, response):
        print('in parse_category')
        products = response.xpath('//div[@class="s-main-slot s-result-list s-search-results sg-row"]//div[contains(@class, "sg-col-4-")]//div[@class="a-section a-spacing-none a-spacing-top-small s-price-instructions-style"]//a/@href').getall()
        for product in products:
            yield SeleniumRequest(url=self.base_url + product , callback=self.parse_product)
        
        next_page = response.xpath('//a[@class="s-pagination-item s-pagination-next s-pagination-button s-pagination-separator"]/@href').get()
        if next_page:
            yield SeleniumRequest(url= self.base_url + next_page , callback=self.category_pages)

    def category_pages(self, response):
        print('in category_pages')
        products = response.xpath('//div[@class="s-main-slot s-result-list s-search-results sg-row"]//div[contains(@class, "sg-col-4-")]//div[@class="a-section a-spacing-none a-spacing-top-small s-price-instructions-style"]//a/@href').getall()
        for product in products:
            yield SeleniumRequest(url= self.base_url + product , callback=self.parse_product)

        next_page = response.xpath('//a[@class="s-pagination-item s-pagination-next s-pagination-button s-pagination-separator"]/@href').get()
        if next_page:
            yield SeleniumRequest(url= self.base_url + next_page , callback=self.category_pages)

    def parse_product(self, response):
        discount_percentage = response.xpath('//span[@class="a-size-large a-color-price savingPriceOverride aok-align-center reinventPriceSavingsPercentageMargin savingsPercentage"]/text()').get()
        if discount_percentage:
            item = {}
            item["user_id"] = self.user_id
            item["country_id"] = self.country_id
            item["status"] = self.status
            item["retailer_id"] = self.retailer_id
            product_name = response.xpath('//span[@id="productTitle"]/text()').get()
            item['product_name'] = product_name.strip()
            item["prod_url"] = response.url
            item["cat_url"] = self.url
            try:
                item['brand_name'] = self.brand_name(response)
            except:
                item['brand_name'] = 'None'
            item['prod_image'] = response.xpath('//div[@id="imgTagWrapperId"]/img/@src').get()
            reviews = self.reviews(response)
            item['reviews'] = reviews
            item['discounted_percent'] = response.xpath('//span[@class="a-size-large a-color-price savingPriceOverride aok-align-center reinventPriceSavingsPercentageMargin savingsPercentage"]/text()').get().strip()
            item['discounted_price'] = self.discounted_price(response)
            pattern = r'/dp/([A-Z0-9]+)'
            match = re.search(pattern, response.url)
            
            if match:
                item['asin'] = match.group(1)
                print(item['asin'])
            else:
                item['asin'] = 'there was no asin found'

            item['listed_price'] = response.xpath('//span[@class="a-price a-text-price"]/span[@class="a-offscreen"]/text()').get()
            try:
                item['product_desc'] = self.descr(response)
            except:
                item['product_desc'] = 'no item description'
            
            item['product_category'] = self.scrapping_urls_cat
            item['discounted'] = 'yes'
            if item['discounted_price'] < item['listed_price']:
                yield item

    def brand_name(self, response):
        name = response.xpath('//a[@id="bylineInfo"]/text()').get()
        final_name = ''
        if 'Visit the' in name:
            final_name = name.replace('Visit the', '')
            final_name = final_name.replace('Store', '')
            return final_name
        elif 'Brand: ' in name:
            final_name = name.replace('Brand: ', '')
            final_name = final_name.replace(' Brand', '')
            return final_name  
    
    def descr(self, response):
        desc = ''
        desc_temp = response.xpath('//div[@id="feature-bullets"]/ul//text()[normalize-space()] | //div[@class="a-expander-content a-expander-extend-content a-expander-content-expanded"]/ul//text()[normalize-space()]').getall()
        for i in desc_temp:
            desc = desc + ' ' + i +' '
        return desc.strip()    

    def discounted_price(self, response):
        disc = response.xpath('//span[@class="a-price aok-align-center reinventPricePriceToPayMargin priceToPay"]/span[@class="a-offscreen"]/text()').get()
        return disc
    
    def reviews(self, response):
        spans = response.xpath('//div[contains(@id, "customer_review")]')
        finals = []
        for i in spans:
            item = {}
            item['review'] = i.xpath('normalize-space(.//div[@data-hook="review-collapsed"]/span)').get()
            stars = i.xpath('.//a[@data-hook="review-title"]/i/@class').get()
            item['stars'] = ""
            if stars:
                item['stars'] = int((stars.split("a-icon-star a-star-")[1].split(" review-rating")[0]).strip())
            finals.append(item)
        return finals