import datetime
from parsers import *


class PajekFactory(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())

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
    date = luigi.DateParameter(default=datetime.date.today())

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
