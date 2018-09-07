from collections import Counter
from ccjob import CommonCrawlJob
from sets import Set

class WordCount(CommonCrawlJob):
    def process_record(self, record):
        if record['Content-Type'] != 'text/plain':
            return
        dictionary = Set([])
        data = record.payload.read()
        for word, count in Counter(data.split()).iteritems():
            dictionary.add(word)

        for word in dictionary:
            yield word, 1
                        
        self.increment_counter('commoncrawl', 'processed_pages', 1)
        


if __name__ == '__main__':
    WordCount.run()