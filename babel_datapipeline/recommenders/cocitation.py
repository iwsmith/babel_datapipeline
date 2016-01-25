#!/usr/bin/env python
current_idx = 0
FILE_DELIM = [' ', ',', "\t"]

def invert_dict(d):
    return dict(zip(d.itervalues(), d.iterkeys()))

def get_next_id():
    global current_idx
    current_idx += 1
    return current_idx - 1

def main(dimension, infile, outfile, cmdline=False, delimiter=' ', numRecs=10):
    from collections import defaultdict
    import itertools
    from scipy.sparse import dok_matrix
    import numpy as np

    if not cmdline:
        infile = open(infile, 'r')

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

    returnString = []

    for i in xrange(S.shape[0]):
        row = S.getrow(i).tocoo()
        recs = [(j, v) for j, v in itertools.izip(row.col, row.data)]
        # A paper shouldn't recommend itself
        recs = filter(lambda x: x[0] != i, recs)
        recs.sort(key=lambda x: x[1], reverse=True)
        for entry in recs[:numRecs]:
            sEntry = "{0} {1} {2}\n".format(paper_ids[i],
                                                      paper_ids[entry[0]],
                                                      entry[1])
            if cmdline:
                outfile.write(sEntry)
            else:
                returnString.append(sEntry)

    if not cmdline:
        return ''.join(returnString)

if __name__ == "__main__":
    import argparse
    import sys
    parser = argparse.ArgumentParser()
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('outfile', nargs='?', type=argparse.FileType('w'), default=sys.stdout)
    parser.add_argument('dimension', type=int, help="Dimension of matrix. Matrix is square")
    parser.add_argument('-n', type=int, help="Max number of recommendations to generate per-paper", default=10)
    parser.add_argument('-d', '--delimiter', type=str, help="Delimiter used in the link file", choices=FILE_DELIM, default=' ')

    args = parser.parse_args()

    main(args.dimension, args.infile, args.outfile, True, args.delimiter, args.n)