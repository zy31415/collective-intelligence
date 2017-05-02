# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os
import sqlite3


class WikipediaPipeline(object):

    db_file = './data.db'

    def __init__(self):

        if os.path.exists(self.db_file):
            os.remove(self.db_file)

        self.conn = sqlite3.connect('./data.db')
        self.cursor = self.conn.cursor()

        # Store url
        self.cursor.execute('create table urllist(url)')

        # Store word
        self.cursor.execute('create table wordlist(word)')

        # Store word location
        self.cursor.execute('create table wordlocation(urlid, wordid, location)')

        # Link table, from => to
        self.cursor.execute('create table link(fromid integer, toid integer)')

        # linkwords: uses thw wordid and linked columns to store which words are actually used in that link.
        self.cursor.execute('create table linkwords(wordid, linkid)')

        # Indexing
        self.cursor.execute('create index wordidx on wordlist(word)')
        self.cursor.execute('create index urlidx on urllist(url)')
        self.cursor.execute('create index wordurlidx on wordlocation(wordid)')
        self.cursor.execute('create index urltoidx on link(toid)')
        self.cursor.execute('create index urlfromidx on link(fromid)')
        self.conn.commit()

    def process_item(self, item, spider):

        url = item['url']

        self.cursor.execute('insert into urllist values (?)', (url,))
        self.conn.commit()

        return item

    def open_spider(self, spider):
        pass


