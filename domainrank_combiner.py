from collections import Counter
from domainrank import DomainRank
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


class DomainRankCombiner(DomainRank):
    def combiner(self, key, values):
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
        yield key, json.dumps(['node', node])

if __name__ == '__main__':
    DomainRankCombiner.run()