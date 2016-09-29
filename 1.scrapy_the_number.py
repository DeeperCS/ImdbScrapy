import scrapy
import lxml
import cPickle as pickle
import json

# scrapy runspider scrapy_the_number.py 

class ImdbSpider(scrapy.Spider):
    name = 'the-numbers.com'
    
    start_urls = ['http://www.the-numbers.com/movie/budgets/all']

    def parse(self, response):
        base_link = 'http://www.the-numbers.com'
        '''
        filename = response.url.split("/")[-2] + '.html'
        with open(filename, 'wb') as f:
            f.write(response.body)
        '''
        trList = response.xpath('//div[@id="page_filling_chart"]/center//table/tr')
        
        seq = 1
        print len(trList)
        
        import pdb
        pdb.set_trace()
        
        movieLst = list()
        for tr in trList:
            item = dict()
            td = tr.xpath('td')
            if len(td)>0:
                releaseDate = td[1].xpath('a/text()').extract()[0]
                link = base_link + td[2].xpath('b/a/@href').extract()[0]
                title = td[2].xpath('b/a/text()').extract()[0]
                productionBudget = td[3].xpath('text()').extract()[0]
                domesticGross = td[4].xpath('text()').extract()[0]
                worldwideGross = td[5].xpath('text()').extract()[0]
                
                item['releaseDate'] = releaseDate
                item['link'] = link
                item['title'] = title
                item['productionBudget'] = productionBudget
                item['domesticGross'] = domesticGross
                item['worldwideGross'] = worldwideGross
                movieLst.append(item)
                
                print seq, '.  ', releaseDate, '.  ', title
                seq += 1
                
        # Save to pkl
        with open('titles.pkl', 'wb') as f:
            pickle.dump(movieLst, f)
            
        # Save to json  
        with open('titles.json', 'wb') as f:
            json.dump(movieLst, f, indent=1)
