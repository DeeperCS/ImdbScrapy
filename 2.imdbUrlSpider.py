# generate real movie links by using imdb search

import scrapy
from scrapy import signals
import json
import cPickle as pickle
import urllib2
import time
import random
import pdb

class ImdbUrlItem(scrapy.Item):
    idx = scrapy.Field()
    link = scrapy.Field()

class JsonWriterPipeline(object):

    def __init__(self):
        self.outputFileName = 'links_output.json'
        self.count = 0
        self.data = []
        
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline
        
    def spider_opened(self, spider):
        print ('#############################JsonWriterPipeline spider opened')
        self.fileHandle = open(self.outputFileName, 'wb')
        
    def spider_closed(self, spider):
        print ('#############################JsonWriterPipeline spider closed')
        # pdb.set_trace()
        json.dump(self.data, self.fileHandle, indent=1)
        self.fileHandle.close()
        print ('#############################Saved to file:{}'.format(self.outputFileName))
        print('Saved and Closed')
        
    def process_item(self, item, spider):
        print self.count
        self.count += 1
        # pdb.set_trace()        
        self.data.append(dict(item))
        return item
    
class SpiderMiddleware(object):

    def __init__(self):
        self.count = 0
        
    def process_spider_exception(self, response, exception, spider):
        print ('#############################Exception')
        # pdb.set_trace()
        
class DownloaderMiddleware(object):

    def __init__(self):
        self.count = 0
        self.outputFileName = 'exceptions_output.json'
        self.err_urls = []
    
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline
    
    def spider_opened(self, spider):
        print ('#############################DownloaderMiddleware spider opened')
        self.fileHandle = open(self.outputFileName, 'wb')
        
    def spider_closed(self, spider):
        print ('#############################DownloaderMiddleware spider closed')
        # pdb.set_trace()
        json.dump(self.err_urls, self.fileHandle, indent=1)
        self.fileHandle.close()
        print ('#############################Saved to file:{}'.format(self.outputFileName))
        print('Saved and Closed')
        
    def process_exception(self, request, exception, spider):
        print ('#############################Downloader Exception')
        print ('exception count:', self.count)
        self.err_urls.append(request.url)
        '''
        with open(self.outputFileName, wb) as f:
            json.dump(self.err_urls, f, indent=1)
        '''
        self.count += 1
        # pdb.set_trace()
        
        
class ProxyMiddleware(object):
    def process_request(self, request, spider):
        print 'Proxy Setting...'
        # request.meta['proxy'] = "http://52.78.39.63:3128"
        
        
class ImdbUrlSpider(scrapy.Spider):
    name = 'imdb.com'
    
    custom_settings = {
        'DOWNLOAD_DELAY' : '10' , 
        'AUTOTHROTTLE_START_DELAY' : '10',
        'AUTOTHROTTLE_MAX_DELAY' : '60' ,
        'CONCURRENT_REQUESTS' : '1' ,
        'ITEM_PIPELINES': {'testSpider.JsonWriterPipeline':300},
        'SPIDER_MIDDLEWARES' : {'testSpider.SpiderMiddleware': 543},
        'DOWNLOADER_MIDDLEWARES' : {
            'testSpider.DownloaderMiddleware': 544, 
            'testSpider.ProxyMiddleware': 510
            },
        'COOKIES_ENABLED': False,
    }
    
    
    # 'SPIDER_MIDDLEWARES':{}
    count = 0
    start_urls = []
    
    ###################
    # prepare start urls
    ###################
    
    titlesFile = 'titles.json'
    
    with open(titlesFile, 'rb') as f:
        titles = json.load(f)
        
    for item in titles:
        movieTitle = item['title']
        movieTitle = urllib2.quote(movieTitle.encode('utf-8'))
        imdb_search_link = "http://www.imdb.com/find?ref_=nv_sr_fn&q={}&s=tt".format(movieTitle)
        start_urls.append(imdb_search_link)

    # start_urls = ['https://www.baidu.com/index.html', 'https://ddddd']
    print('start_urls total length:', len(start_urls))

    pdb.set_trace()
    
    def start_requests(self):
        for url in self.start_urls:
            # time.sleep(random.random()*2)  # random sleep 0-2 seconds
            self.count += 1

            #print('count:{}  sleep'.format(self.count), '.'*10)
            #time.sleep(random.random()*5+0.5)
            
            yield self.make_requests_from_url(url)
            
            
    def make_requests_from_url(self, url):
        return scrapy.Request(url, dont_filter=True)

    
    def parse(self, response):
        item = ImdbUrlItem()
        base_url = 'http://www.imdb.com'
        # pdb.set_trace()
        findList = response.xpath('//table[@class="findList"]/tr')
        link = ''
        if len(findList)>0:
            detailUrl = findList[0].xpath('td')[1].xpath('a/@href').extract()[0]
            link = base_url + detailUrl
            item['link'] = link
        else:
            item['link'] = 'Error'
        # print link
        yield item