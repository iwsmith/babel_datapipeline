import datetime
import luigi
from io import *


class AMinerParse(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return AminerS3Targets()

    def output(self):
        return luigi.LocalTarget(path='citation_dict/aminer_parse_%s.txt' % self.date)

    def run(self):
        from babel_util.parsers import aminer
        p = aminer.AMinerParser()
        with self.output().open('w') as outfile:
            with self.input().open('r') as infile:
                for paper in p.parse(infile):
                    for citation in paper["citations"]:
                        outfile.write("{0} {1}\n".format(paper["id"], citation))
