from collections import Counter
from ccjob import CommonCrawlJob

class WordCount(CommonCrawlJob):
    def process_record(self, record):
        if record['Content-Type'] != 'text/plain':
            return
        data = record.payload.read()
        self.increment_counter('commoncrawl', 'processed_pages', 1)
        yield record['WARC-Target-URI'], 1


if __name__ == '__main__':
    WordCount.run()