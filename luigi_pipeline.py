# In order to run with luigi command, this folder must be added to sys.path

import aminer as am
import bibcouple as bib
import cocitation as cocite
import ef
import luigi
import luigi.s3 as s3

class LocalTargetInputs(luigi.ExternalTask):
    def output(self):
        return luigi.file.LocalTarget(path='targets/aminer_single.txt')

class AminerS3Targets(luigi.Task):
    def output(self):
        aws_id, key = '', ''
        for line in open('~/.boto', 'r'):
            parts = line.split(' = ')
            if parts[0] == 'aws_access_key_id':
                aws_id = parts[1]
            elif parts[0] == 'aws_secret_access_key':
                key = parts[1]
        s3client = s3.S3Client(aws_access_key_id=aws_id,
                               aws_secret_access_key=key)
        return s3.S3Target(path='citation-databases/Aminer/raw/aminer.paper.gz', client=s3client)

class AMinerParse(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return LocalTargetInputs()

    def output(self):
        return luigi.LocalTarget(path='citation_dict/aminer_parse_%s.txt' % self.date)

    def run(self):
        p = am.AMinerParser()
        with self.output().open('w') as outfile:
            with self.input().open('r') as infile:
                for paper in p.parse(infile):
                    for citation in paper["citations"]:
                        outfile.write("{0} {1}\n".format(paper["id"], citation))

class CocitationTask(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return AMinerParse(date = self.date)

    def output(self):
        return luigi.LocalTarget(path='recs/cocitation_%s.txt' % self.date)

    def run(self):
        with self.output().open('w') as outfile:
            with self.input().open('r') as infile:
                distinct = set()
                for line in infile:
                    paperIDs = line.split(' ')
                    distinct.add(paperIDs[0])
                    distinct.add(paperIDs[1])
                dim = len(distinct)
                outfile = cocite.main(dim, self.input(), self.output())

class BibcoupleTask(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return AMinerParse(date = self.date)

    def output(self):
        return luigi.LocalTarget(path='recs/bibcouple%s.txt' % self.date)

    def run(self):
        with self.output().open('w') as outfile:
            with self.input().open('r') as infile:
                distinct = set()
                for line in infile:
                    paperIDs = line.split(' ')
                    distinct.add(paperIDs[0])
                    distinct.add(paperIDs[1])
                dim = len(distinct)
                outfile = bib.main(dim, self.input(), self.output())

class EFnxTask(luigi.task):
    #need to make ef_nx write to a file

    date = luigi.DateParameter()

    def requires(self):
        return AMinerParse(date = self.date)

    def output(self):
        return luigi.LocalTarget(path='recs/ef_nx%s.txt' % self.date)

    def run(self):
        from ef_nx import EFExpert
        with self.output().open('w') as outfile:
            with self.input().open('r') as infile:
                ef = EFExpert(None)
                ef.converter(infile)
                ef.recommend('''paperID''')

class EFTask(luigi.task):
    date = luigi.DateParameter()

    def requires(self):
        return AMinerParse(date = self.date)

    def output(self):
        return


