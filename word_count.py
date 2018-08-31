from collections import Counter
from ccjob import CommonCrawlJob

class WordCount(CommonCrawlJob):
    def process_record(self, record):
        data = record.payload.read()
        self.increment_counter('commoncrawl', 'processed_pages', 1)
        yield record['Content-Type'], 1


if __name__ == '__main__':
    WordCount.run()