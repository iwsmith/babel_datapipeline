import luigi
from babel_datapipeline.tasks.parsers import *
from babel_datapipeline.tasks.infomap import *
from babel_datapipeline.util.misc import *


class CocitationTask(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return AMinerParse(date=self.date)

    def output(self):
        makedir('recs')
        return luigi.LocalTarget(path='recs/cocitation_%s.txt' % self.date)

    def run(self):
        from babel_util.recommenders import cocitation
        with open(self.output().path, 'w') as outfile:
            with open(self.input().path, 'r') as infile:
                dim = countnodes(infile)
                cocitation.main(dim, infile, outfile,
                                delimiter=' ')


class BibcoupleTask(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return AMinerParse(date=self.date)

    def output(self):
        makedir('recs')
        return luigi.LocalTarget(path='recs/bibcouple_%s.txt' % self.date)

    def run(self):
        from babel_util.recommenders import bibcouple
        with open(self.output().path, 'w') as outfile:
            with open(self.input().path, 'r') as infile:
                dim = countnodes(infile)
                bibcouple.main(dim, infile, outfile,
                                delimiter=' ')


class EFTask(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return InfomapTask(date=self.date)

    def output(self):
        makedir('recs')
        return (luigi.LocalTarget(path='recs/ef_classic_%s.txt' % self.date),
                luigi.LocalTarget(path='recs/ef_expert_%s.txt' % self.date))

    def run(self):
        from babel_util.recommenders import ef
        with open(self.output()[0].path, 'w') as classic:
            with open(self.output()[1].path, 'w') as expert:
                with open(self.input()[0].path, 'r') as infile:
                    ef.main(infile, classic, expert)


