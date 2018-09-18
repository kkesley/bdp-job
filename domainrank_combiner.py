from collections import Counter
from domainrank import DomainRank
from urlparse import urlparse
from mrjob.step import MRStep
import logging
import json
import re
import string
import math

# Regex for checking if a string is a url
regex = re.compile(
    r'^(?:http|ftp)s?://' # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' # domain...
    r'localhost|' # localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|' # ...or ipv4
    r'\[?[A-F0-9]*:[A-F0-9:]+\]?)' # ...or ipv6
    r'(?::\d+)?' # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)

# Initialize LOG
logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)


class DomainRankCombiner(DomainRank):
    def combiner(self, key, values):
        """
        Override basic combiner to act like reducer
        """
        # initialize values
        scores = list(values)
        score_val = 0
        node = {
            "score": 0,
            "links": []
        }
        for score in scores:
            scoreDict = json.loads(score)
            if scoreDict[0] == "score": #if the key of the tuple is score, update the temporary score
                score_val += scoreDict[1]
            else:
                node = scoreDict[1] #if the key of the tuple is node, update current node to this node
                if "links" not in node:
                    node["links"] = []
        
        node['score'] = score_val #update the node score to the temporary score
        yield key, json.dumps(['node', node]) #yield the node

if __name__ == '__main__':
    DomainRankCombiner.run()