import gzip
import logging
import boto3
import botocore
import warc
from gzipstream import GzipStreamFile
from tempfile import TemporaryFile
from mrjob.job import MRJob
from mrjob.util import log_to_stream
import json
logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)
class CommonCrawlJob(MRJob):
    def configure_options(self):
        super(CommonCrawlJob, self).configure_options()
        self.add_passthrough_option('--s3_local_temp_dir',
                                    help='local temporary directory to buffer content from S3',
                                    default=None)
                                    
    def process_record(self, record):
        """
        process each record from Common Crawl file. Must be implemented
        """
        raise NotImplementedError('process_record needs to be overriden')

    def mapper(self, _, line):
        """
        The Map of MapReduce, pulls the CommonCrawl files from S3
        """
        # Connect to Amazon S3 using anonymous credentials
        boto_config = botocore.client.Config(
            signature_version=botocore.UNSIGNED,
            read_timeout=180,
            retries={'max_attempts' : 20})
        s3client = boto3.client('s3', config=boto_config)
        # Verify bucket
        try:
            s3client.head_bucket(Bucket='commoncrawl')
        except botocore.exceptions.ClientError as exception:
            LOG.error('Failed to access bucket "commoncrawl": %s', exception)
            return
        # Check whether WARC/WAT/WET input exists
        try:
            s3client.head_object(Bucket='commoncrawl',
                                    Key=line)
        except botocore.client.ClientError as exception:
            LOG.error('Input not found: %s', line)
            return
        # Start a connection to one of the WARC/WAT/WET files
        LOG.info('Loading s3://commoncrawl/%s', line)
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
        for _i, record in enumerate(ccfile):
            if record['Content-Type'] != 'application/json':
                continue
            payload = record.payload.read()
            data = json.loads(payload)
            
            if data['Envelope']['WARC-Header-Metadata']['WARC-Type'] != 'response':
                continue
            for link in data['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Links']:
                LOG.info('%s', link)
            for key, value in self.process_record(record):
                yield key, value
            self.increment_counter('commoncrawl', 'processed_records', 1)

    def combiner(self, key, values):
        """
        Combiner of MapReduce
        Default implementation just calls the reducer which does not make
        it necessary to implement the combiner in a derived class. Only
        if the reducer is not "associative", the combiner needs to be
        overwritten.
        """
        for key_val in self.reducer(key, values):
            yield key_val

    def reducer(self, key, values):
        """
        The Reduce of MapReduce
        If you're trying to count stuff, this `reducer` will do. If you're
        trying to do something more, you'll likely need to override this.
        """
        yield key, sum(values)