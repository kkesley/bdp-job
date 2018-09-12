from collections import Counter
from ccjob import CommonCrawlJob
from sets import Set

class CoOcurence(CommonCrawlJob):
    def process_record(self, record):
        if record['Content-Type'] != 'text/plain':
            return
        dictionary = {}
        data = record.payload.read()
        for word, count in Counter(data.split()).iteritems():
            if word.lower() not in dictionary:
                dictionary[word.lower()] = 0
            dictionary[word.lower()] += 1

        for word in dictionary:
            yield word, dictionary[word]
                        
        self.increment_counter('commoncrawl', 'processed_pages', 1)
    
    def combiner(self, key, values):
        dictionary = {}
        for value in values:
            for word, item in value.items():
                if word not in dictionary:
                    dictionary[word] = 0
                dictionary[word] += item
        yield key, dictionary
    
    def reducer(self, key, values):
        dictionary = {}
        for value in values:
            for word, item in value.items():
                if word not in dictionary:
                    dictionary[word] = 0
                dictionary[word] += item
        yield key, dictionary

        


if __name__ == '__main__':
    CoOcurence.run()