import gzip
import logging
import boto3
import botocore
import warc
from gzipstream import GzipStreamFile
from tempfile import TemporaryFile
from mrjob.job import MRJob
from mrjob.util import log_to_stream
import time
import sys
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
        self.start_time = time.time()
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
        sys.stderr.write("Downloading s3://commoncrawl/{}\n".format(line))
        sys.stderr.write(time.strftime("Download [START]. Distance from initial time: %Hh:%Mm:%Ss\n", time.gmtime(time.time() - self.start_time)))
        try:
            temp = TemporaryFile(mode='w+b',
                                    dir=self.options.s3_local_temp_dir)
            s3client.download_fileobj('commoncrawl', line, temp)
        except botocore.client.ClientError as exception:
            LOG.error('Failed to download %s: %s', line, exception)
            return
        sys.stderr.write(time.strftime("Download [FINISHED]. Distance from initial time: %Hh:%Mm:%Ss\n", time.gmtime(time.time() - self.start_time)))
        temp.seek(0)
        ccfile = warc.WARCFile(fileobj=(GzipStreamFile(temp)))
        sys.stderr.write('Attempting MapReduce Job......\n')
        sys.stderr.write(time.strftime("Processing [START]. Distance from initial time: %Hh:%Mm:%Ss\n", time.gmtime(time.time() - self.start_time)))
        for _i, record in enumerate(ccfile):
            for key, value in self.process_record(record):
                yield key, value
            self.increment_counter('commoncrawl', 'processed_records', 1)
        sys.stderr.write(time.strftime("Processing [FINISHED]. Distance from initial time: %Hh:%Mm:%Ss\n", time.gmtime(time.time() - self.start_time)))

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
        sys.stderr.write(time.strftime("Combiner [START]. Distance from initial time: %Hh:%Mm:%Ss\n", time.gmtime(time.time() - self.start_time)))
        yield key, sum(values)
        sys.stderr.write(time.strftime("Combiner [FINISHED]. Distance from initial time: %Hh:%Mm:%Ss\n", time.gmtime(time.time() - self.start_time)))