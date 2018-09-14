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
                yield key, json.dumps(['score', float(value) / link_count * source_score])
            yield domain, json.dumps(['node', {
                "links": valid_links,
                "score": source_score
            }])
            self.increment_counter('commoncrawl', 'processed page', 1)
        except KeyError:
            pass

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
            yield key, json.dumps(['score', float(value) / link_count * source_score])
        yield src, json.dumps(['node', {
            "links": links,
            "score": source_score
        }])

    def combiner(self, key, values):
        scores = list(values)
        for score in scores:
            yield key, score

    def reducer(self, key, values):
        scores = list(values)
        score_val = 0
        node = {
            "score": 0,
            "links": []
        }
        for score in scores:
            scoreDict = json.loads(score)
            if scoreDict[0] == "score":
                score_val += scoreDict[1]
            else:
                node = scoreDict[1]
                if "links" not in node:
                    node["links"] = []
        
        node['score'] = score_val
        yield key, json.dumps(node)

    def sorting_mapper(self, key, line):
        record = json.loads(line)
        source_score = record["score"]
        yield get_prefix(int(record["score"])) + "_" + '{:f}'.format(record["score"]), key

    def sorting_reducer(self, key, values):
        for url in values:
            yield url, key.split("_")[1]

    def steps(self):
        return [MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer)] + \
        [MRStep(mapper=self.scoring_mapper, combiner=self.combiner, reducer=self.reducer, jobconf={"mapred.reduce.tasks": 10})] * 1 + \
        [MRStep(mapper=self.sorting_mapper, reducer=self.sorting_reducer, jobconf={"mapred.reduce.tasks": 1})]
        

if __name__ == '__main__':
    DomainRank.run()