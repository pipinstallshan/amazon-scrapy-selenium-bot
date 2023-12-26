import os
import mysql.connector
from dotenv import load_dotenv
from scrapy.exceptions import DropItem


class CrawlPipeline:
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

    def process_item(self, item, spider):
        # _id = item['_id']
        user_id = item['user_id']
        country_id = item['country_id']
        url = item['cat_url']
        status = item['status']
        retailer_id = item['retailer_id']
        product_category = item['product_category']
        
        try:
            update_prods = "INSERT INTO product (title, prod_url, cat_url, description, price, discount, status, country_id, brandname, retailer_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            prods_val = (
                item['product_name'],
                item['prod_url'],
                url,
                item['product_desc'],
                item['listed_price'],
                item['discounted_price'],
                status,
                country_id,
                item['brand_name'],
                retailer_id
            )
            self.cursor.execute(update_prods, prods_val)
            last_product_id = self.cursor.lastrowid
            
            update_prod_cat = "INSERT INTO product_category (product_id, product_category) VALUES (%s, %s)"
            prod_cat_val = (last_product_id, product_category)
            self.cursor.execute(update_prod_cat, prod_cat_val)
            
            update_images = "INSERT INTO product_image (product_id, src, filesrc) VALUES (%s, %s, %s)"
            images_val = (last_product_id, item['prod_image'], None)           
            self.cursor.execute(update_images, images_val)
            
            for i in item['reviews']:
                update_review = "INSERT INTO product_review (product_id, review, stars) VALUES (%s, %s, %s)"
                review_val = (last_product_id, i['review'], i['stars'])
                self.cursor.execute(update_review, review_val)
            
            self.db.commit()
            print('Data inserted into products MariaDB')
            return item
        
        except Exception as e:
            print(f"Error inserting data into MariaDB: {e}")
            raise DropItem("Item dropped due to database error")

    def close_spider(self, spider):
        self.db.close()