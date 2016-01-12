# In order to run with luigi command, this folder must be added to sys.path

import luigi
import luigi.s3 as s3

import parsers.aminer as am
import recommenders.bibcouple as bib
import recommenders.cocitation as cocite

class LocalTargetInputs(luigi.ExternalTask):
    def output(self):
        return luigi.file.LocalTarget(path='local_raw_targets/aminer.paper')

class AminerS3Targets(luigi.Task):
    def output(self):
        s3client = s3.S3Client()
        return s3.S3Target(path='s3://citation-databases/Aminer/raw/aminer.paper.gz', client=s3client)

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
                dim = countPapers(infile)
                outfile = cocite.main(dim, infile, outfile, delimiter=' ')

class BibcoupleTask(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return AMinerParse(date = self.date)

    def output(self):
        return luigi.LocalTarget(path='recs/bibcouple%s.txt' % self.date)

    def run(self):
        with self.output().open('w') as outfile:
            with self.input().open('r') as infile:
                dim = countPapers(infile)
                outfile = bib.main(dim, self.input(), outfile, delimiter=' ')


def countPapers(infile):
    distinct = set()
    for line in infile:
        paperIDs = line.split(' ')
        distinct.add(paperIDs[0])
        distinct.add(paperIDs[1])
    return len(distinct)


