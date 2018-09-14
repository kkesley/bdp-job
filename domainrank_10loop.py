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

class DomainRankTenLoop(DomainRank):
    def steps(self):
        return [MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer)] + \
        [MRStep(mapper=self.scoring_mapper, combiner=self.combiner, reducer=self.reducer, jobconf={"mapred.reduce.tasks": 10})] * 10 + \
        [MRStep(mapper=self.sorting_mapper, reducer=self.sorting_reducer, jobconf={"mapred.reduce.tasks": 1})]
        

if __name__ == '__main__':
    DomainRankTenLoop.run()