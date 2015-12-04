import luigi

from recommenders import bibcouple as bib

if __name__ == "__main__":

    f = luigi.file.LocalTarget(path='../citation_dict/aminer_parse_2015-12-04.txt')
    print(f)

    outfile = open('test_output.txt', 'w')

    distinct = set()
    for line in f.open('r'):
        paperIDs = line.split(' ')
        distinct.add(paperIDs[0])
        distinct.add(paperIDs[1])
    dim = len(distinct)

    bib.main(dim, f, outfile, delimiter=' ')