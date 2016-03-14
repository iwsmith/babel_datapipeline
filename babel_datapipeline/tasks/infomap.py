import datetime
from babel_datapipeline.tasks.parsers import *
import os
from babel_datapipeline.util.misc import *
from subprocess import check_output, check_call, STDOUT


class PajekFactory(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return AMinerParse(date=self.date)

    def output(self):
        makedir('pajek_files')
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
    dataset = 'aminer'  # TODO make this a luigi parameter
    outfolder_loc = 'infomap_output'
    generic_path = '%s/%s_pajek' % (outfolder_loc, dataset)

    def requires(self):
        return PajekFactory(date=self.date)

    def output(self):
        makedir(self.outfolder_loc)
        return (luigi.LocalTarget(path='%s_%s.tree.gz' % (self.generic_path, self.date)),
                luigi.LocalTarget(path='%s_%s.bftree.gz' % (self.generic_path, self.date)),
                luigi.LocalTarget(path='%s_%s.map.gz' % (self.generic_path, self.date)))

    def run(self):
        infomap_loc = 'Infomap'
        infile_loc = self.input().path
        infomap_options = '--tree --map --bftree -t -N 1'
        infomap_log = check_output('%s %s %s %s' % (infomap_loc, infile_loc, self.outfolder_loc, infomap_options),
                                 stderr=STDOUT, shell=True)

        s3client = s3.S3Client()
        s3client.put_string(infomap_log, 'S3://babel-logging/%s_infomap_output_%s.txt' % (self.dataset, self.date))

        for extension in ('tree', 'bftree', 'map'):
            file_path = '%s_%s.%s' % (self.generic_path, self.date, extension)
            check_call(['gzip', file_path])
            file_name = '%s_pajek_%s.%s.gz' % (self.dataset, self.date, extension)
            s3client.put('%s.gz' % file_path, 'S3://babel-processing/%s' % file_name)