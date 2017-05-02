# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os
import sqlite3

import nltk


class WikipediaPipeline(object):

    db_file = './data.db'

    stop_words = set(nltk.corpus.stopwords.words('english'))

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

        referer = item['referer']
        url = item['url']
        self._insert_url(url)

        referer_id = self._url_id_lookup(referer)
        url_id = self._url_id_lookup(url)

        self.cursor.execute('insert into link values (?, ?)', (referer_id, url_id))
        self.conn.commit()

        content = item['content']

        tokenizer = nltk.tokenize.RegexpTokenizer(r'\w+')

        words = tokenizer.tokenize(content)

        for location, word in enumerate(words):
            word = word.lower()
            if word in self.stop_words:
                continue
            word_id = self._get_word_id(word)

            self.cursor.execute('insert into wordlocation(urlid, wordid, location) values (?, ?, ?)',
                              (url_id, word_id, location))

        self.conn.commit()

        return item

    def _insert_url(self, url):
        self.cursor.execute('insert into urllist values (?)', (url,))
        self.conn.commit()

    def _url_id_lookup(self, url):
        self.cursor.execute('select rowid from urllist where url=?', (url,))
        url_id = self.cursor.fetchall()
        assert len(url_id) == 1 and len(url_id[0]) == 1, "Returned url id has wrong format."

        return int(url_id[0][0])

    def _get_word_id(self, word):
        cur = self.cursor.execute('select rowid from wordlist where word=?', (word,))
        res = cur.fetchall()
        if len(res) == 0:
            cur = self.conn.execute('insert into wordlist values(?)', (word,))
            return int(cur.lastrowid)

        else:
            assert len(res) == 1, "Wrong return."
            return int(res[0][0])



