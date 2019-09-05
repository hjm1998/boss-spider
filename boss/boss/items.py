# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class BossItem(scrapy.Item):
    company = scrapy.Field()
    position = scrapy.Field()
    salary = scrapy.Field()
    city = scrapy.Field()
    experience = scrapy.Field()
    education = scrapy.Field()
    describes = scrapy.Field()
    tags = scrapy.Field()
    origin_url = scrapy.Field()
