import json
import scrapy
from itemadapter import ItemAdapter
from scrapy.crawler import CrawlerProcess
from scrapy.item import Item, Field
from seed import check_and_load_data


class AuthorItem(Item):
    fullname = Field()
    born_date = Field()
    born_location = Field()
    description = Field()


class QuoteItem(Item):
    quote = Field()
    author = Field()
    tags = Field()


class DataPipeline:
    def open_spider(self, spider):
        self.quotes = []
        self.authors = []

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if 'fullname' in adapter.keys():
            self.authors.append(adapter.asdict())
        if 'quote' in adapter.keys():
            self.quotes.append(adapter.asdict())
        return item

    def close_spider(self, spider):
        with open('quotes.json', 'w', encoding='utf-8') as fd:
            json.dump(self.quotes, fd, ensure_ascii=False, indent=2)
        with open('authors.json', 'w', encoding='utf-8') as fd:
            json.dump(self.authors, fd, ensure_ascii=False, indent=2)


class QuotesSpider(scrapy.Spider):
    name = "get_quotes"
    allowed_domains = ["quotes.toscrape.com"]
    start_urls = ["http://quotes.toscrape.com/"]
    custom_settings = {"ITEM_PIPELINES": {"__main__.DataPipeline": 300}}

    def parse(self, response):
        for q in response.xpath("//div[@class='quote']"):
            quote = q.xpath("span[@class='text']/text()").get()
            author = q.xpath("span/small[@class='author']/text()").get()
            tags = q.xpath("div[@class='tags']/a/text()").extract()
            tags = [tag.strip().lower() for tag in tags]

            yield QuoteItem(quote=quote.strip() if quote else None,
                            author=author.strip() if author else None,
                            tags=tags)

            author_url = q.xpath("span/a/@href").get()
            if author_url:
                yield response.follow(author_url, callback=self.parse_author)

        next_page = response.xpath("//li[@class='next']/a/@href").get()
        if next_page:
            yield scrapy.Request(response.urljoin(next_page), callback=self.parse)

    def parse_author(self, response):
        content = response.xpath("//div[@class='author-details']")
        fullname = content.xpath("h3[@class='author-title']/text()").get()
        born_date = content.xpath("p/span[@class='author-born-date']/text()").get()
        born_location = content.xpath("p/span[@class='author-born-location']/text()").get()
        description = content.xpath("div[@class='author-description']/text()").get()

        return AuthorItem(fullname=fullname.strip() if fullname else None,
                          born_date=born_date.strip() if born_date else None,
                          born_location=born_location.strip() if born_location else None,
                          description=description.strip() if description else None)


if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(QuotesSpider)
    process.start()

    db_created = check_and_load_data('quotes.json', 'authors.json', 'hw_9', 'mongodb://localhost:27017/')

    if db_created:
        print("База даних успішно створена.")
    else:
        print("База даних вже існує.")