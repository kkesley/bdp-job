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
                valid_links.append(link)
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

    def reducer(self, key, values):
        scores = list(values)
        score_val = 0
        node = {}
        for score in scores:
            scoreDict = json.loads(score)
            if scoreDict[0] == "score":
                score_val += scoreDict[1]
            else:
                node = scoreDict[1]
        
        node['score'] = score_val
        yield key, json.dumps(node)
        

if __name__ == '__main__':
    DomainRank.run()