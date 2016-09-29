# parse movie items from given urls

import scrapy
from scrapy import signals
import json
import cPickle as pickle
import urllib2
import time
import random
import pdb

class MovieItem(scrapy.Item):
    # 11 basis 
    imdb_link = scrapy.Field()
    movie_title = scrapy.Field()
    imdb_score = scrapy.Field()
    
    movie_year = scrapy.Field()
    num_voted_users = scrapy.Field()
    content_rating = scrapy.Field()
    duration = scrapy.Field()
    
    director_info = scrapy.Field()
    
    image_urls = scrapy.Field()

    num_user_for_reviews = scrapy.Field()
    num_critic_for_reviews = scrapy.Field()
    
    num_facebook_like = scrapy.Field()
    
    meta_score = scrapy.Field()
    popularity = scrapy.Field()
    
    writers = scrapy.Field()
    
    # 3 Storyline
    plot_keywords = scrapy.Field()
    storyline = scrapy.Field()
    genres = scrapy.Field()
    
    # 5 Details
    country = scrapy.Field()
    language = scrapy.Field()
    filming_locations = scrapy.Field()
    
    # Box Office
    budget = scrapy.Field()
    opening_weekend = scrapy.Field()
    gross = scrapy.Field()
    
    # 1 Technical Specs
    color = scrapy.Field()
    aspect_ratio = scrapy.Field()
    
    # 1 Cast
    cast_info = scrapy.Field()
    

class JsonWriterPipeline(object):

    def __init__(self):
        self.outputFileName = 'imdb_info_output.json'
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
        
        
class imdbInfoSpider(scrapy.Spider):
    name = 'imdb.com'
    count = 0
    base_url = 'http://www.imdb.com'
    
    custom_settings = {
        'DOWNLOAD_DELAY' : '10' , 
        'AUTOTHROTTLE_START_DELAY' : '10',
        'AUTOTHROTTLE_MAX_DELAY' : '60' ,
        'CONCURRENT_REQUESTS' : '1' ,
        'ITEM_PIPELINES': {'imdbInfoSpider.JsonWriterPipeline':300},
        'SPIDER_MIDDLEWARES' : {'imdbInfoSpider.SpiderMiddleware': 543},
        'DOWNLOADER_MIDDLEWARES' : {
            'imdbInfoSpider.DownloaderMiddleware': 544, 
            'imdbInfoSpider.ProxyMiddleware': 510
            },
        'COOKIES_ENABLED': False,
    }
    
    ###################
    # prepare start urls
    ###################
    linksFile = 'movieImdbLinks.json'
    
    with open(linksFile, 'rb') as f:
        links = json.load(f)
        
    start_urls = [l for l in links]

    # start_urls = start_urls[100:300]
    print("length:", len(start_urls))

    pdb.set_trace()
        
    def parse(self, response):
        self.count += 1
            
        item = MovieItem()
        item['imdb_link'] = response.url
        
        # 1.movie_title----------------------------------------------------------
        try:
            item['movie_title'] = response.xpath('//div[@class="title_wrapper"]/h1/text()').extract()[0].strip()
        except:
            item['movie_title'] = None
            
        
        # 2.imdb_score----------------------------------------------------------
        try:
            item['imdb_score'] = response.xpath('//span[@itemprop="ratingValue"]/text()').extract()[0].strip()
        except:
            item['imdb_score'] = None
            
        
        # 3.movie_year----------------------------------------------------------
        try:
            item['movie_year'] =  response.xpath('//span[@id="titleYear"]/a/text()').extract()[0].strip()
        except:
            item['movie_year'] = None
            
            
        # 4.num_voted_users----------------------------------------------------------
        try:
            item['num_voted_users'] = response.xpath('//span[@itemprop="ratingCount"]/text()').extract()[0].strip()
        except:
            item['num_voted_users'] = None
            
            
        # 5.content_rating----------------------------------------------------------
        try:
            item['content_rating'] = response.xpath('//meta[@itemprop="contentRating"]/@content').extract()[0].strip()
        except:
            item['content_rating'] = None
            
            
        # 6.duration----------------------------------------------------------
        try:
            item['duration'] = response.xpath('//time[@itemprop="duration"]/text()').extract()[0].strip()
        except:
            item['duration'] = None
            
        
        # 7.director_info----------------------------------------------------------
        try:
            info = {}
            info['director_name'] = response.xpath('//span[@itemprop="director"]/a/span/text()').extract()[0].strip()
            info['director_link'] = self.base_url + response.xpath('//span[@itemprop="director"]/a/@href').extract()[0].strip()
            info['director_facebook_likes'] = None
            
            item['director_info'] = info
        except:
            item['director_info'] = None
            
            
        # 8.image_urls----------------------------------------------------------
        try:
            item['image_urls'] = response.xpath('//div[@class="poster"]/a/img/@src').extract()[0].strip()
        except:
            item['image_urls'] = None
            

        # 9.num_user_for_reviews----------------------------------------------------------
        try:
            item['num_user_for_reviews'] = response.xpath('//div[@class="titleReviewBarItem titleReviewbarItemBorder"]/div')[1].xpath('span/a/text()')[0].extract().strip().split(" ")[0]
        except:
            item['num_user_for_reviews'] = None
            
            
        # 10.num_critic_for_reviews----------------------------------------------------------
        try:
            item['num_critic_for_reviews'] = response.xpath('//div[@class="titleReviewBarItem titleReviewbarItemBorder"]/div')[1].xpath('span/a/text()')[1].extract().strip().split(" ")[0]
        except:
            item['num_critic_for_reviews'] = None
            
            
        # *** 11.num_facebook_like----------------------------------------------------------
        try:
            item['num_facebook_like'] = None
        except:
            item['num_facebook_like'] = None
         
        
        # 12.meta_score----------------------------------------------------------
        try:
            item['meta_score'] = response.xpath('//div[@class="metacriticScore score_favorable titleReviewBarSubItem"]/span/text()').extract()[0]
        except:
            item['meta_score'] = None
            
            
        # 13.popularity----------------------------------------------------------
        try:
            item['popularity'] = response.xpath('//div[contains(text(), "Popularity")]/following-sibling::node()/span/text()').extract()[0].strip().split('\n')[0]
        except:
            item['popularity'] = None
            
        # 13.writers----------------------------------------------------------
        try:
            item['writers'] = response.xpath('//div[@id="title-overview-widget"]//span[@itemprop="creator"]/a/span/text()').extract()
        except:
            item['writers'] = None  
            
            
        ####################    Story line    ######################
        # 1.plot_keywords----------------------------------------------------------
        try:            
            item['plot_keywords'] = response.xpath('//span[@itemprop="keywords"]/text()').extract()
        except:
            item['plot_keywords'] = None
            
        # 2.storyline----------------------------------------------------------
        try:
            item['storyline'] = response.xpath('//div[@id="titleStoryLine"]/div[@itemprop="description"]/p/text()').extract()[0]
        except:
            item['storyline'] = None
            
        # 3.genres----------------------------------------------------------
        try:
            item['genres'] = response.xpath('//div[@itemprop="genre"]/a/text()').extract()
        except:
            item['genres'] = None
         
        
        ####################    Details    ######################
        # 1.country----------------------------------------------------------
        try:
            item['country'] = response.xpath('//div[@id="titleDetails"]/div/a[contains(@href, "country")]/text()').extract()[0].strip()
        except:
            item['country'] = None
            
        # 2.language----------------------------------------------------------
        try:
            item['language'] = response.xpath('//div[@id="titleDetails"]/div/a[contains(@href, "language")]/text()').extract()[0].strip()
        except:
            item['language'] = None
            
        # 3.filming_locations----------------------------------------------------------
        try:
            item['filming_locations'] = response.xpath('//h4[contains(text(), "Filming Locations:")]/following-sibling::node()/text()')[0].extract()
        except:
            item['filming_locations'] = None
            
        # 4.color----------------------------------------------------------
        try:
            item['color'] = response.xpath('//a[contains(@href, "color")]/text()').extract()[0]
        except:
            item['color'] = None
        
            
        ####################    Box Office    ######################
            
        # 1.budget----------------------------------------------------------
        try:
            item['budget'] = response.xpath('//h4[contains(text(), "Budget:")]/following-sibling::node()/descendant-or-self::text()').extract()
        except:
            item['budget'] = None
            
        
        # 2.opening_weekend----------------------------------------------------------
        try:
            item['opening_weekend'] = response.xpath('//h4[contains(text(), "Budget:")]/following-sibling::node()/descendant-or-self::text()').extract()
        except:
            item['opening_weekend'] = None
            
            
        # 3.gross----------------------------------------------------------
        try:
            item['gross'] = response.xpath('//h4[contains(text(), "Opening Weekend:")]/following-sibling::node()/descendant-or-self::text()').extract()
        except:
            item['gross'] = None
            
            
        ####################    Technical Specs    ######################
        # 1.aspect_ratio----------------------------------------------------------
        try:
            item['aspect_ratio'] = response.xpath('//h4[contains(text(), "Aspect Ratio:")]/following-sibling::text()').extract()[0].strip()
        except:
            item['aspect_ratio'] = None
            
            
        ####################    Cast    ######################
        # 1.cast_info----------------------------------------------------------
        try:
            item['cast_info'] = response.xpath('//div[@id="titleCast"]//span[@itemprop="name"]/text()').extract()
        except:
            item['cast_info'] = None
            
        # print link
        yield item