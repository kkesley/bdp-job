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



class DomainRank(CommonCrawlJob):
    alphabet = list(string.ascii_uppercase+string.ascii_lowercase)
    maxAlphabet = len(alphabet)
    def get_prefix(self, num):
        """
        Get prefix of a score to sort it in the output
        """
        idx = max(int(math.log(max(num, 1), 10)), 0) #Use base 10 log to determine the alphabet character
        idx = min(idx, self.maxAlphabet - 1) #if idx more than 52, use the latest index (very large number not supported. 52 digits is way too long)
        return self.alphabet[idx]

    def configure_args(self):
        """
        Configure iterations and sort flag to an instance variable
        """
        super(DomainRank, self).configure_args()

        self.add_passthru_arg(
            '--iterations', dest='iterations', default=1, type=int,
            help='number of iterations to run') # configure iterations flag

        self.add_passthru_arg(
            '--sortrank', dest='sortrank', default=1, type=int,
            help='sort or no sort') # configure sort flag

    def process_record(self, record):
        """
        Process record (must be WAT file)
        """
        if record['Content-Type'] != 'application/json': # content type must be a json
            return
        payload = record.payload.read()
        data = json.loads(payload)
        if data['Envelope']['WARC-Header-Metadata']['WARC-Type'] != 'response': # must be a response
            return
        
        try:
            url = data['Envelope']['WARC-Header-Metadata']['WARC-Target-URI'] # server url
            domain = urlparse(url).netloc
            domain = domain.replace("www.", "") # strip the url only for a domain. Hence the name DomainRank
            links = data['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Links'] # get all the links
            source_score = 1
            valid_links = []
            destination_count = {}
            for link in links:
                if re.match(regex, link["url"]) is None:
                    continue
                domain_link = urlparse(link["url"]).netloc
                domain_link = domain_link.replace("www.", "") # strip the links to domain level
                if domain == domain_link: # if it's referring to an internal page, don't score the link
                    continue

                if domain_link not in destination_count: # if not exist in dict, just initialize it to 0
                    destination_count[domain_link] = 0
                destination_count[domain_link]+=1 # add the number of referring links

                valid_links.append(domain_link) #append the link to valid links array

            link_count = len(valid_links) #total number of valid links

            for key, value in destination_count.iteritems():
                yield key, json.dumps(['score', float(value) / link_count * source_score]) # yield a score which is normalized with the link_count and its score

            yield domain, json.dumps(['node', {
                "links": valid_links,
                "score": source_score
            }]) # yield a node to support iterative scoring
            self.increment_counter('commoncrawl', 'processed page', 1) #update mapreduce counter
        except KeyError:
            pass

    def scoring_mapper(self, src, value):
        """
        Second mapper used to score the links. Can be iterated.
        """
        record = json.loads(value) # the mapper input has to be a node

        source_score = record["score"] # get the current score of a link
        links = []
        if "links" in record:
            links = record["links"] # get all referral links

        destination_count = {}
        for link in links:
            if link not in destination_count:
                destination_count[link] = 0
            destination_count[link]+=1 #count the links

        link_count = len(links)
        for key, value in destination_count.iteritems():
            yield key, json.dumps(['score', float(value) / link_count * source_score]) # yield a score which is normalized with the link_count and its score
        yield src, json.dumps(['node', {
            "links": links,
            "score": source_score
        }]) # yield a node to support iterative scoring

    def combiner(self, key, values):
        """
        Combiner just yield original yielded value
        """
        scores = list(values)
        for score in scores:
            yield key, score

    def reducer(self, key, values):
        """
        Reducer to process scoring
        """
        #initialize values
        scores = list(values)
        score_val = 0
        node = {
            "score": 0,
            "links": []
        }
        for score in scores:
            scoreDict = json.loads(score)
            if scoreDict[0] == "score": #if the key of tuple is score, then it's a score
                score_val += scoreDict[1] #update the temporary score from referring links
            else:
                node = scoreDict[1] #if the key of tuple is node, it's a node, update our node
                if "links" not in node: #if not exists, link must be empty array
                    node["links"] = []
        
        node['score'] = score_val #update the score of this link
        yield key, json.dumps(node) #yield the node

    def sorting_mapper(self, key, line):
        """
        Final mapper to sort the values into the output. Switch the score value into key and append prefix to sort it automatically in reducer.
        """
        record = json.loads(line)
        source_score = record["score"]
        yield self.get_prefix(int(record["score"])) + "_" + '{:f}'.format(record["score"]), key

    def sorting_reducer(self, key, values):
        """
        Final reducer just combine values from mapper and output it to a file
        """
        for url in values:
            yield key, url

    def steps(self):
        """
        override steps to run 3 tier job.
        """
        job = [MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer)] + \
        [MRStep(mapper=self.scoring_mapper, combiner=self.combiner, reducer=self.reducer, jobconf={"mapred.reduce.tasks": 10})] * self.options.iterations #run the second tier n times (depends on the iterations flag)

        if self.options.sortrank is not 0: #if sort flag is equal to False, don't use the 3rd tier. (output won't be sort)
            job = job + [MRStep(mapper=self.sorting_mapper, reducer=self.sorting_reducer, jobconf={"mapred.reduce.tasks": 1})]

        return job
        
        

if __name__ == '__main__':
    DomainRank.run()