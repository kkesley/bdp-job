from collections import Counter
from domainrank import DomainRank
from urlparse import urlparse
from mrjob.step import MRStep
import logging
import json
import re
import string
import math
import gzip
import logging
import boto3
import botocore
import warc
from gzipstream import GzipStreamFile
from tempfile import TemporaryFile
from mrjob.job import MRJob
from mrjob.util import log_to_stream

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

class DomainRankInMapper(DomainRank):
    def process_record_init(self):
        self.urls = {} #initialize dictionary

    def mapper(self, _, line):
        """
        Override default mapper. Not yielding anything
        """
        # Connect to Amazon S3 using anonymous credentials
        boto_config = botocore.client.Config(
            signature_version=botocore.UNSIGNED,
            read_timeout=180,
            retries={'max_attempts' : 20})
        s3client = boto3.client('s3', config=boto_config)
        # Check if the bucket exist
        try:
            s3client.head_bucket(Bucket='commoncrawl')
        except botocore.exceptions.ClientError as exception:
            LOG.error('Failed to access bucket "commoncrawl": %s', exception)
            return
        # Check if the input exists
        try:
            s3client.head_object(Bucket='commoncrawl',
                                    Key=line)
        except botocore.client.ClientError as exception:
            LOG.error('Input not found: %s', line)
            return
        # Download input files
        LOG.info('Downloading s3://commoncrawl/%s', line)
        try:
            temp = TemporaryFile(mode='w+b',
                                    dir=self.options.s3_local_temp_dir)
            s3client.download_fileobj('commoncrawl', line, temp)
        except botocore.client.ClientError as exception:
            LOG.error('Failed to download %s: %s', line, exception)
            return
        temp.seek(0)
        ccfile = warc.WARCFile(fileobj=(GzipStreamFile(temp)))
        LOG.info('Attempting MapReduce Job......')
        for _i, record in enumerate(ccfile): #don't yield the result
            self.process_record(record)
            self.increment_counter('commoncrawl', 'processed_records', 1)

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

            for key, value in destination_count.iteritems(): #update the dictionary to hold all urls in this record
                if key not in self.urls:
                    self.urls[key] = {
                        "links": [],
                        "score": 0
                    }
                self.urls[key]["score"] += float(value) / link_count * source_score #update the score of the target node
            if domain not in self.urls:
                self.urls[domain] = {
                    "links": [],
                    "score": 0
                }
            self.urls[domain]["links"] = self.urls[domain]["links"] + valid_links #update the links of source the node
            self.urls[domain]["score"] = source_score #update the score of source node
            self.increment_counter('commoncrawl', 'processed page', 1)
        except KeyError:
            pass
    
    def process_record_final(self):
        """
        Combine all values from dictionary and yield it.
        """
        for key, value in self.urls.iteritems():
            yield key, json.dumps(['node', value]) #yield the value of every item in dictionary

    def scoring_mapper(self, src, value):
        """
        Override mapper to use dictionary instead of yielding the value
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

        for key, value in destination_count.iteritems(): #update the dictionary to hold all urls in this record
            if key not in self.urls:
                self.urls[key] = {
                    "links": [],
                    "score": 0
                }
            self.urls[key]["score"] += float(value) / link_count * source_score #update the score of the target node
        if src not in self.urls:
            self.urls[src] = {
                "links": [],
                "score": 0
            }
        self.urls[src]["links"] = self.urls[src]["links"] + links #update the links of the source node
        self.urls[src]["score"] = source_score #update the score of source node

    def steps(self):
        """
        Override steps to use mapper_final method
        """
        return [MRStep(mapper_init=self.process_record_init, mapper=self.mapper, mapper_final=self.process_record_final, combiner=self.combiner, reducer=self.reducer)] + \
        [MRStep(mapper_init=self.process_record_init, mapper=self.scoring_mapper, mapper_final=self.process_record_final, combiner=self.combiner, reducer=self.reducer, jobconf={"mapred.reduce.tasks": 10})] * 1 + \
        [MRStep(mapper=self.sorting_mapper, reducer=self.sorting_reducer, jobconf={"mapred.reduce.tasks": 1})]
        

if __name__ == '__main__':
    DomainRankInMapper.run()