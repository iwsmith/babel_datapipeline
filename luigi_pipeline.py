# In order to run with luigi command, this folder must be added to sys.path

import luigi
import luigi.s3 as s3

import parsers.aminer as am

class LocalTargetInputs(luigi.ExternalTask):
    def output(self):
        return luigi.file.LocalTarget(path='local_raw_targets/aminer.paper')

class AminerS3Targets(luigi.Task):
    def output(self):
        s3client = s3.S3Client()
        gformat = luigi.format.GzipFormat()
        return s3.S3Target(path='S3://citation-databases/Aminer/raw/aminer.paper.gz', format=gformat, client=s3client)

class AMinerParse(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return AminerS3Targets()

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
        import recommenders.cocitation as cocite
        with self.output().open('w') as outfile:
            with open(self.input().path, 'r') as infile:
                dim = countPapers(infile)
                outString = cocite.main(dim, self.input().path, open(self.output().path, 'w'),
                                        delimiter=' ', numRecs=-1)
                outfile.write(outString)

class BibcoupleTask(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return AMinerParse(date = self.date)

    def output(self):
        return luigi.LocalTarget(path='recs/bibcouple%s.txt' % self.date)

    def run(self):
        import recommenders.bibcouple as bib
        with self.output().open('w') as outfile:
            with open(self.input().path, 'r') as infile:
                dim = countPapers(infile)
                outString = bib.main(dim, self.input().path, open(self.output().path, 'w'),
                                     delimiter=' ', numRecs=-1)
                outfile.write(outString)

class DynamoOutputTask(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return BibcoupleTask(date = self.date)

    def run(self):
        from database.transformer import main
        main('aminer', open(self.input().path, 'r'), create=True,flush=True)


def countPapers(infile):
    distinct = set()
    for line in infile:
        paperIDs = line.split(' ')
        distinct.add(paperIDs[0])
        distinct.add(paperIDs[1])
    return len(distinct)


