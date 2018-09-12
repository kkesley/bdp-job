from collections import Counter
from ccjob import CommonCrawlJob
import json

class PageRank(CommonCrawlJob):
    def process_record(self, record):
        if record['Content-Type'] != 'application/json':
            return
        payload = record.payload.read()
        data = json.loads(payload)
        if data['Envelope']['WARC-Header-Metadata']['WARC-Type'] != 'response':
            return
        
        try:
            for link in data['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Links']:
                yield link['url'], 1     
            self.increment_counter('commoncrawl', 'processed page', 1)
        except KeyError:
            pass
        
if __name__ == '__main__':
    PageRank.run()