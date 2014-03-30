# -*- coding: utf-8 -*-
__author__ = 'David'

import urllib2
import sqlite3
import re
##
from bs4 import BeautifulSoup
from urlparse import urljoin

class spider:


    def __init__(self, dbname):
        self.conn = sqlite3.connect(dbname)

    def __del__(self):
        self.conn.close()

    def create_index_tables(self):
        self.conn.execute('create table urllist(url)')
        self.conn.execute('create table wordlist(word)')
        self.conn.execute('create table wordlocation(urlid,wordid)')
        self.conn.execute('create table link(fromid integer,toid integer)')
        self.conn.execute('create table linkwords(wordid,linkid)')
        self.conn.execute('create index wordidx on wordlist(word)')
        self.conn.execute('create index urlidx on urllist(url)')
        self.conn.execute('create index wordurlidx on wordlocation(wordid)')
        self.conn.execute('create index urltoidx on link(toid)')
        self.conn.execute('create index urlfromidx on link(fromid)')

    def dbcommit(self):
        self.conn.commit()

    def crawl(self, pages, depth=2):

        for i in range(depth):
            newpages = set()
            # pages - list of links
            for page in pages:
                try:
                    c = urllib2.urlopen(page)
                except:
                    print "Unable to open " + page
                    continue
                soup = BeautifulSoup(c.read())
                self.add_to_index(page, soup)

                links = soup('a')
                for link in links:
                    if 'href' in link.attrs:
                        url = urljoin(page, link['href'])
                        if url[0:4] == 'http':
                            newpages.add(url)
            pages = newpages
            self.dbcommit()

    def get_text_only(self, soup):
        v = soup.string
        if v is None:
            content = soup.contents
            text = ""
            for element in content:
                subtext = self.get_text_only(element)
                text += subtext + '\n'
            return text
        else:
            return v.strip()

    def separate_words(self, text):
        template = re.compile('\W+', re.UNICODE)
        words = []
        for s in template.split(text):
            words.append(s.lower())
        return set(words)

    def add_to_index(self, url, soup):
        print "Индексируется " + url

        text = self.get_text_only(soup)
        words = self.separate_words(text)

        url_id = self.get_entry_id('urllist', 'url', url)
        for word in words:
            word_id = self.get_entry_id('wordlist', 'word', word)
            self.conn.execute("insert into wordlocation(urlid, wordid) values (%d, %d)"
                              % (url_id, word_id))


    def get_entry_id(self, table, field, value):
        c = self.conn.execute("select rowid from " + table + " where " + field + "='" + value +"'")
        res = c.fetchone()
        if res is None:
            c = self.conn.execute("insert into " + table + "("+ field + ") values ('" + value +"')")
            return c.lastrowid
        else:
            return res[0]

    def is_indexed(self, url):
        c = self.conn.execute("select rowid from urllist where url='"+url+"'")
        res = c.fetchone()
        if res is not None:
            idxd = self.conn.execute("select * from wordlocation where urlid='"+res[0]+"'")
            v = idxd.fetchone()
            if v is not None:
                return True
        else:
            return False



def search(conn, query):
    query = query.lower()
    words = query.split(' ')
    word_ids = []
    all_url_ids = set()
    urls = []
    for word in words:
        word_row = conn.execute("select rowid from wordlist where word='"+word+"'")
        word_row = word_row.fetchone()
        url_ids = set()
        if word_row is not None:
            all_urls = conn.execute("select urlid from wordlocation where wordid=%d" % word_row[0])
            for url_id in all_urls.fetchall():
                url_ids.add(url_id[0])
        if not all_url_ids:
            all_url_ids.symmetric_difference_update(url_ids)
        else:
            all_url_ids.intersection_update(url_ids)

    for url_id in all_url_ids:
        url = conn.execute("select url from urllist where rowid=%d" % url_id)
        url = url.fetchone()
        if url is not None:
            urls.append(url[0])

    return urls



c = spider('db.dbl')
c.create_index_tables()
c.crawl(['http://argus-rs.no/'], depth=2)

conn = sqlite3.connect('db.dbl')
print search(conn, "impressive manufacturer")
conn.close()