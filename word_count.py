from collections import Counter
from ccjob import CommonCrawlJob

class WordCount(CommonCrawlJob):
    def process_record(self, record):
        data = record.payload.read()
        for word, count in Counter(data.split()).iteritems():
            yield word, 1

        self.increment_counter('commoncrawl', 'processed_pages', 1)


if __name__ == '__main__':
    WordCount.run()