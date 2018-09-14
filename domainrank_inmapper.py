from collections import Counter
from ccjob import CommonCrawlJob
from urlparse import urlparse
from mrjob.step import MRStep
import logging
import json
import re
import string
import math
regex = re.compile(
    r'^(?:http|ftp)s?://' # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' # domain...
    r'localhost|' # localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|' # ...or ipv4
    r'\[?[A-F0-9]*:[A-F0-9:]+\]?)' # ...or ipv6
    r'(?::\d+)?' # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

alphabet = list(string.ascii_uppercase+string.ascii_lowercase)
maxAlphabet = len(alphabet)
def get_prefix(num):
    idx = max(int(math.log(max(num, 1), 10)) - 1, 0)
    idx = min(idx, maxAlphabet - 1)
    return alphabet[idx]

class DomainRankInMapper(DomainRank):
    def process_record_init(self):
        self.urls = {}

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
            domain = domain.replace("www.", "")
            links = data['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Links']
            source_score = 1
            valid_links = []
            destination_count = {}
            for link in links:
                if re.match(regex, link["url"]) is None:
                    continue
                domain_link = urlparse(link["url"]).netloc
                domain_link = domain_link.replace("www.", "")
                if domain == domain_link:
                    continue
                if domain_link not in destination_count:
                    destination_count[domain_link] = 0
                destination_count[domain_link]+=1
                valid_links.append(domain_link)
            link_count = len(valid_links)
            for key, value in destination_count.iteritems():
                if key not in self.urls:
                    self.urls[key] = {
                        "links": [],
                        "score": 0
                    }
                self.urls[key].score += float(value) / link_count * source_score
            if domain not in self.urls:
                self.urls[domain] = {
                    "links": [],
                    "score": 0
                }
            self.urls[domain].links = self.urls[domain].links + valid_links
            self.urls[domain].score = source_score
            self.increment_counter('commoncrawl', 'processed page', 1)
        except KeyError:
            pass
    
    def process_record_final(self):
        for key, value in self.urls.iteritems()
            yield key, json.dumps(['node', value])

    def scoring_mapper(self, src, value):
        record = json.loads(value)

        source_score = record["score"]
        links = []
        if "links" in record:
            links = record["links"]

        destination_count = {}
        for link in links:
            if link not in destination_count:
                destination_count[link] = 0
            destination_count[link]+=1

        link_count = len(links)
        for key, value in destination_count.iteritems():
            if key not in self.urls:
                self.urls[key] = {
                    "links": [],
                    "score": 0
                }
            self.urls[key].score += float(value) / link_count * source_score
        if domain not in self.urls:
            self.urls[domain] = {
                "links": [],
                "score": 0
            }
        self.urls[domain].links = self.urls[domain].links + links
        self.urls[domain].score = source_score

    def steps(self):
        return [MRStep(mapper_init=self.process_record_init, mapper=self.mapper, mapper_final=self.process_record_final, combiner=self.combiner, reducer=self.reducer)] + \
        [MRStep(mapper_init=self.process_record_init, mapper=self.scoring_mapper, mapper_final=self.process_record_final, combiner=self.combiner, reducer=self.reducer, jobconf={"mapred.reduce.tasks": 10})] * 1 + \
        [MRStep(mapper=self.sorting_mapper, reducer=self.sorting_reducer, jobconf={"mapred.reduce.tasks": 1})]
        

if __name__ == '__main__':
    DomainRankInMapper.run()