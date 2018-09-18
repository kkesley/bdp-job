import gzip
import logging
import boto3
import botocore
import warc
from gzipstream import GzipStreamFile
from tempfile import TemporaryFile
from mrjob.job import MRJob
from mrjob.util import log_to_stream
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
        process each record from the input file. Must be implemented
        """
        raise NotImplementedError('process_record needs to be overriden')

    def mapper(self, _, line):
        """
        The map will download the file from commoncrawl, parse the file into multiple records, and process each record
        """
        # Connect to Amazon S3 using anonymous credentials
        boto_config = botocore.client.Config(
            signature_version=botocore.UNSIGNED,
            read_timeout=180,
            retries={'max_attempts' : 20})
        s3client = boto3.client('s3', config=boto_config)
        # Check bucket existence
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
        # Download input
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
        for _i, record in enumerate(ccfile):
            for key, value in self.process_record(record):
                yield key, value
            self.increment_counter('commoncrawl', 'processed_records', 1)

    def combiner(self, key, values):
        """
        Basic combiner just call the reducer
        """
        for key_val in self.reducer(key, values):
            yield key_val

    def reducer(self, key, values):
        """
        Basic reducer just aggregate values of a key. Implement new reducer if the value is not an integer
        """
        yield key, sum(values)