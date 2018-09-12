from collections import Counter
from ccjob import CommonCrawlJob
from urlparse import urlparse
import json
import re
regex = re.compile(
    r'^(?:http|ftp)s?://' # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' # domain...
    r'localhost|' # localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|' # ...or ipv4
    r'\[?[A-F0-9]*:[A-F0-9:]+\]?)' # ...or ipv6
    r'(?::\d+)?' # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)

class PageRank(CommonCrawlJob):
    def process_record(self, record):
        if record['Content-Type'] != 'application/json':
            return
        payload = record.payload.read()
        data = json.loads(payload)
        if data['Envelope']['WARC-Header-Metadata']['WARC-Type'] != 'response':
            return
        
        try:
            url = data['Envelope']['WARC-Header-Metadata']['WARC-Target-URI']
            domain = urlparse(url).netloc
            for link in data['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Links']:
                if re.match(regex, link["url"]) is None:
                    continue
                domain_link = urlparse(link["url"]).netloc
                if domain != domain_link:
                    yield link["url"], 1
            self.increment_counter('commoncrawl', 'processed page', 1)
        except KeyError:
            pass
        
if __name__ == '__main__':
    PageRank.run()