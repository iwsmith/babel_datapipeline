#!/usr/bin/env python
current_idx = 0
FILE_DELIM = [' ', ',', "\t"]


def invert_dict(d):
    return dict(zip(d.itervalues(), d.iterkeys()))


def get_next_id():
    global current_idx
    current_idx += 1
    return current_idx - 1


def main(dimension, infile, outfile, delimiter=None, numRecs=10):
    from collections import defaultdict
    import itertools
    from scipy.sparse import dok_matrix
    import numpy as np

    infile = infile.open('r')
    # outfile = open(outfile, 'w')

    # debuglog = open('debuglog.txt', 'w')
    # debuglog.write('something happened!\n')
    # debuglog.write(str(dimension) + '\n')
    # debuglog.write('delimiter: \'' + str(delimiter) + '\'\n')
    # debuglog.write('numRecs: ' + str(numRecs) + '\n')

    infile = list(infile)
    # debuglog.write('infile: \n' + str(infile[:5]))

    S = dok_matrix((dimension, dimension), dtype=np.uint8)
    paper_ids = defaultdict(get_next_id)

    reader = map(str.strip, infile)
    #reader = itertools.imap(lambda x: map(str.strip, x.split(delimiter)), list(infile))

    for paper_cites in reader:
        paper_cites = paper_cites.split(delimiter)
        paper = paper_cites[0]
        cites = paper_cites[1]
        # debuglog.write(paper + ' ' + cites + '\n')
        S[paper_ids[paper], paper_ids[cites]] = 1

    S = S.tocsr()
    S = S.dot(S.T)

    paper_ids = invert_dict(paper_ids)

    for i in xrange(S.shape[0]):
        row = S.getrow(i).tocoo()
        recs = [(j, v) for j, v in itertools.izip(row.col, row.data)]
        # A paper shouldn't recommend itself
        recs = filter(lambda x: x[0] != i, recs)
        recs.sort(key=lambda x: x[1], reverse=True)
        for entry in recs[:numRecs]:
            outfile.write("{0} {1} {2}\n".format(paper_ids[i],
                                                 paper_ids[entry[0]],
                                                 entry[1]))

    # debuglog.close()
    return outfile
