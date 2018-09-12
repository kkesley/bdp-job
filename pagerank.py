from collections import Counter
from ccjob import CommonCrawlJob

class PageRank(CommonCrawlJob):
    def process_record(self, record):
        yield "", 1                        
        self.increment_counter('commoncrawl', 'processed_pages', 1)

if __name__ == '__main__':
    PageRank.run()