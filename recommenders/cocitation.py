#!/usr/bin/env python
current_idx = 0
FILE_DELIM = [' ', ',', "\t"]

def invert_dict(d):
    return dict(zip(d.itervalues(), d.iterkeys()))

def get_next_id():
    global current_idx
    current_idx += 1
    return current_idx - 1

def main(dimension, infile, outfile, delimiter='\t', numRecs=10):
    from collections import defaultdict
    import itertools
    from scipy.sparse import dok_matrix
    import numpy as np

    # outfile = open(outfile, 'w')
    # infile = open(infile, 'r')

    S = dok_matrix((dimension, dimension), dtype=np.uint8)
    paper_ids = defaultdict(get_next_id)
    
    reader = itertools.imap(lambda x: map(str.strip, x.split(delimiter)), infile)

    grouped_reader = itertools.groupby(reader, lambda x: x[0])
    for _, citations in grouped_reader:
        for _, p1 in citations:
            for _, p2 in citations:
                if p1 != p2:
                    S[paper_ids[p1], paper_ids[p2]] += 1

    S = S.tocsr()

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
