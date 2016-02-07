import luigi
import luigi.s3 as s3
from babel_util.parsers import aminer


class LocalTargetInputs(luigi.ExternalTask):
    def output(self):
        return luigi.file.LocalTarget(path='local_raw_targets/aminer.paper')


class AminerS3Targets(luigi.Task):
    def output(self):
        s3client = s3.S3Client()
        gformat = luigi.format.GzipFormat()
        return s3.S3Target(path='S3://citation-databases/Aminer/raw/aminer.paper.gz',
                           format=gformat, client=s3client)


class AMinerParse(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return AminerS3Targets()

    def output(self):
        return luigi.LocalTarget(path='citation_dict/aminer_parse_%s.txt' % self.date)

    def run(self):
        p = aminer.AMinerParser()
        with self.output().open('w') as outfile:
            with self.input().open('r') as infile:
                for paper in p.parse(infile):
                    for citation in paper["citations"]:
                        outfile.write("{0} {1}\n".format(paper["id"], citation))


class PajekFactory(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return AMinerParse(date=self.date)

    def output(self):
        return luigi.LocalTarget(path='pajek_files/aminer_pajek_%s.net' % self.date)

    def run(self):
        from babel_util.util.PajekFactory import PajekFactory

        pjk = PajekFactory()

        with open(self.output().path, 'w') as outfile:
            with open(self.input().path, 'r') as infile:
                for edge in infile:
                    vertices = edge.strip().split(' ')  # TODO add global (argument?) for delimiter
                    pjk.add_edge(vertices[0], vertices[1])
                pjk.write(outfile)


class InfomapTask(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return PajekFactory(date=self.date)

    def output(self):
        return (luigi.LocalTarget(path='infomap_output/aminer_pajek_%s.tree' % self.date),
                luigi.LocalTarget(path='infomap_output/aminer_pajek_%s.bftree' % self.date),
                luigi.LocalTarget(path='infomap_output/aminer_pajek_%s.map' % self.date))

    def run(self):
        from subprocess import check_call, STDOUT
        infomap_loc = 'Infomap'
        infile_loc = self.input().path
        outfolder_loc = 'infomap_output/'
        infomap_options = '--tree --map --bftree -t -N 1'
        check_call('%s %s %s %s' % (infomap_loc, infile_loc, outfolder_loc, infomap_options),
                   stderr=STDOUT, shell=True)


class CocitationTask(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return AMinerParse(date=self.date)

    def output(self):
        return luigi.LocalTarget(path='recs/cocitation_%s.txt' % self.date)

    def run(self):
        from babel_util.recommenders import cocitation
        with open(self.output().path, 'w') as outfile:
            with open(self.input().path, 'r') as infile:
                dim = countPapers(infile)
                cocitation.main(dim, infile, outfile,
                                delimiter=' ')


class BibcoupleTask(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return AMinerParse(date=self.date)

    def output(self):
        return luigi.LocalTarget(path='recs/bibcouple_%s.txt' % self.date)

    def run(self):
        from babel_util.recommenders import bibcouple
        with open(self.output().path, 'w') as outfile:
            with open(self.input().path, 'r') as infile:
                dim = countPapers(infile)
                bibcouple.main(dim, infile, outfile,
                                delimiter=' ')


class EFTask(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return InfomapTask(date=self.date)

    def output(self):
        return (luigi.LocalTarget(path='recs/ef_classic_%s.txt' % self.date),
                luigi.LocalTarget(path='recs/ef_expert_%s.txt' % self.date))

    def run(self):
        from babel_util.recommenders import ef
        with open(self.output()[0].path, 'w') as classic:
            with open(self.output()[1].path, 'w') as expert:
                with open(self.input()[0].path, 'r') as infile:
                    ef.main(infile, classic, expert)


class DynamoOutputTask(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return BibcoupleTask(date=self.date)

    def run(self):
        from database.transformer import main
        main('aminer', open(self.input().path, 'r'), create=True,flush=True)


def countPapers(infile):
    distinct = set()
    for line in infile:
        paperIDs = line.split(' ')
        distinct.add(paperIDs[0])
        distinct.add(paperIDs[1])
    infile.seek(0)
    return len(distinct)


