from ccjob import CommonCrawlJob

class UniqueDomain(CommonCrawlJob):
    def process_record(self, record):
        if record['Content-Type'] != 'text/plain':
            return         
        yield record['WARC-Target-URI'], 1
        


if __name__ == '__main__':
    UniqueDomain.run()