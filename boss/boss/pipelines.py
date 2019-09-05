# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exporters import JsonLinesItemExporter
import pymysql
from twisted.enterprise import adbapi
from pymysql import cursors


class BossMySQLPipeline(object):
    def __init__(self):
        dbparmas = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'root',
            'database': 'boss',
            'charset': 'utf8'
        }
        self.conn = pymysql.Connection(**dbparmas)
        self.cursor = self.conn.cursor()
        self._sql = None

    def process_item(self, item, spider):
        self.cursor.execute(self.sql, (item['company'], item['position'], item['salary'], item['city'],
                                       item['experience'], item['education'], item['describes'], item['tags'],
                                       item['origin_url']))
        self.conn.commit()
        return item

    @property
    def sql(self):
        if not self._sql:
            self._sql = """
            insert into bosstext(id,company,position,salary,city,experience,education,describes,tags,origin_url)
            value(null,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            return self._sql
        return self._sql

class BossTwistedPipeline(object):

    def __init__(self):
        dbparmas = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'root',
            'database': 'boss',
            'charset': 'utf8',
            'cursorclass': cursors.DictCursor
        }
        self.dbpool = adbapi.ConnectionPool('pymysql', **dbparmas)
        self._sql = None

    @property
    def sql(self):
        if not self._sql:
            self._sql = """
            insert into bosstext(id,company,position,salary,city,experience,education,describes,tags,origin_url)
            value(null,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            return self._sql
        return self._sql

    def process_item(self, item, spider):
        defer = self.dbpool.runInteraction(self.insert_item, item)
        defer.addErrback(self.handle_error)

    def insert_item(self, cursor, item):
        cursor.execute(self.sql, (item['company'], item['position'], item['salary'], item['city'],
                                  item['experience'], item['education'], item['describes'], item['tags'],
                                  item['origin_url']))

    def handle_error(self, error):
        print('=' * 30 + 'error' + '=' * 30)
        print(error)
        print('=' * 30 + 'error' + '=' * 30)

# class BossPipeline(object):
#     def __init__(self):
#         self.f = open('content.json', 'wb')
#         self.exporter = JsonLinesItemExporter(self.f, ensure_ascii=False, encoding='utf-8')
#
#     def process_item(self, item, spider):
#         self.exporter.export_item(item)
#         return item
#
#     def close_spider(self, spider):
#         self.f.close()

