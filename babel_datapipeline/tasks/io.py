import luigi
import luigi.s3 as s3
from recommenders import *
import datetime


class LocalTargetInputs(luigi.ExternalTask):
    def output(self):
        return luigi.file.LocalTarget(path='local_raw_targets/aminer.paper')


class AminerS3Targets(luigi.Task):
    def output(self):
        s3client = s3.S3Client()
        gformat = luigi.format.GzipFormat()
        return s3.S3Target(path='S3://citation-databases/Aminer/raw/aminer.paper.gz',
                           format=gformat, client=s3client)


class DynamoOutputTask(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return EFTask(date=self.date)

    def run(self):
        from babel_datapipeline.database.transformer import main
        for infile in self.input():
            main('aminer', open(infile.path, 'r'), create=True,flush=True)