# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from boss.items import BossItem


class ZhipinSpider(CrawlSpider):
    name = 'zhipin'
    allowed_domains = ['www.zhipin.com']
    start_urls = ['https://www.zhipin.com/c101280100/?query=python&page=1']

    rules = (
        Rule(LinkExtractor(allow=r'.+/\?query=.+&page=\d', restrict_xpaths="//div[@class='page']"), follow=True),
        Rule(LinkExtractor(allow=r'.+job_detail.+html', restrict_xpaths="//div[@class='job-list']/ul"),
             callback='parse_detail', follow=False)
    )

    def start_requests(self):
        citys = [ 'c101280600', 'c101280100']
        positions = ['python', 'Java', 'C%2B%2B', 'PHP']
        for city in citys:
            for position in positions:
                url = 'https://www.zhipin.com/{}/?query={}&page=1'.format(city, position)
                yield scrapy.Request(url)

    def parse_detail(self, response):
        company = response.xpath(
            "//div[@class='company-info']/a[@ka='job-detail-company_custompage']/text()").get().strip()
        position = response.xpath("//div[@class='name']/h1/text()").get()
        salary = response.xpath("//span[@class='salary']/text()").get()
        texts = response.xpath("//div[@class='info-primary']/p/text()").getall()
        city = texts[0]
        experience = texts[1]
        education = texts[2]
        describes = "".join(response.xpath("(//div[@class='job-sec'])[1]/div/text()").getall()).strip()
        origin_url = response.url
        tags = ",".join(response.xpath(
            "(//div[@class='tag-more'])[1]/div[contains(@class, 'tag-all')]//text()").getall()[1:-1]).strip()

        item = BossItem(company=company, position=position, salary=salary, city=city, experience=experience,
                        education=education, describes=describes, tags=tags, origin_url=origin_url)
        yield item

