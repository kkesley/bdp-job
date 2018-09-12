from __future__ import division
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

class DomainRank(CommonCrawlJob):
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
            links = data['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Links']
            destination_count = {}
            link_count = 0
            for link in links:
                if re.match(regex, link["url"]) is None:
                    continue
                domain_link = urlparse(link["url"]).netloc
                if domain == domain_link:
                    continue
                if domain_link not in destination_count:
                    destination_count[domain_link] = 0
                destination_count[domain_link]+=1
                link_count +=1
            for key, value in destination_count.iteritems():
                yield key, value / link_count
            self.increment_counter('commoncrawl', 'processed page', 1)
        except KeyError:
            pass

    def reducer(self, key, values):
        yield key, len(list(values)) * sum(values)

if __name__ == '__main__':
    DomainRank.run()