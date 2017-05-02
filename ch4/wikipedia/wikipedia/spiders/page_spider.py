from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

from bs4 import BeautifulSoup

from ..items import WikipediaItem


class PageSpider(CrawlSpider):
    """
    the Page Spider for wikipedia
    """

    name = "wikipedia_pages"
    allowed_domains = ["wikipedia.org"]

    start_urls = ["https://en.wikipedia.org/wiki/Main_Page"]

    rules = (
        Rule(LinkExtractor(allow="https://en\.wikipedia\.org/wiki/.+"),
             callback='parse_wikipedia_page', follow=False),
    )

    def parse_wikipedia_page(self, response):

        item = WikipediaItem()
        soup = BeautifulSoup(response.body, 'lxml')

        item['url'] = response.url
        item['referer'] = response.request.headers['referer'].decode()
        item['name'] = soup.find("h1", {"id": "firstHeading"}).text

        body = soup.find("div", {"id": "bodyContent"})

        content = []

        for p in body.findAll('p'):
            content.append(p.text)

        item['content'] = ' '.join(content)

        return item
