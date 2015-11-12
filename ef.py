#!/usr/bin/env python
"""Implements classic and expert EigenFactor recommendations"""
import itertools

class TreeRecord(object):
    __slots__ = ("doi", "local", "score", "parent")
    def __init__(self, cluster, doi, score):
        cluster = cluster.split(CLUSTER_DELIMITER)

        try:
            cluster.pop() # Remove local order
            self.local = CLUSTER_DELIMITER.join(cluster)
        except IndexError:
            self.local = None
        try:
            cluster.pop() # Remove local-cluster id
            self.parent = CLUSTER_DELIMITER.join(cluster)
        except IndexError:
            self.parent = None

        score = float(score)
        if score == 0:
            score = -1.0 #Dynamo doesn't understand inf

        # Strip whitespace and any quotes
        self.doi = doi.strip().strip('"')
        self.score = score

    def __eq__(self, other):
        return self.doi == other.doi and self.local == other.local and self.parent == other.parent

    def __ne__(self, other):
        return not self == other

class Recommendation(object):
    rec_type = None
    __slots__ = ("target_doi", "doi", "score")

    def __init__(self, target_doi, doi, score):
        self.target_doi = target_doi
        self.score = score
        self.doi = doi

    def to_flat(self):
        return "{0} {1} {2}\n".format(self.target_doi, self.doi, self.score)

    def __str__(self):
        return "%s %s %s %s" % (self.target_doi, self.rec_type, self.doi, self.score)

    def __repr__(self):
        return "<%s,%s,%s,%s>" % (self.target_doi, self.rec_type, self.doi, self.score)

class ClassicRec(Recommendation):
    rec_type = "classic"

class ExpertRec(Recommendation):
    rec_type = "expert"


CLUSTER_DELIMITER = ':'
def require_local(e):
    return e.local != None
def get_local(e):
    return e.local
def get_parent(e):
    return e.parent
def get_score(e):
    return e.score
def make_tree_rec(entry):
    """Transforms a raw entry to a TreeRecord"""
    return TreeRecord(entry[0], entry[2], entry[1])

def make_expert_rec(stream, rec_limit=10, to_int=False):
    """Given a stream of TreeRecord, generate ExpertRecs.

    Args:
        stream: A stream of TreeRecords, sorted by cluster_id (1:1:1, 1:1:2)
        rec_limit: The number of recommendations to generate per-paper. Default 10.
        
    Returns:
        A generator returning a sorted list of ExpertRecs."""
    filtered_stream = itertools.ifilter(require_local, stream)
    expert_stream = itertools.groupby(filtered_stream, get_local)

    for (_, stream) in expert_stream:
        # These are already sorted
        candidates = [e for e in stream]
        score = rec_limit
        for paper in candidates:
            # A paper shouldn't recommend itself
            topn = filter(lambda e: e.doi != paper.doi, candidates[:rec_limit+1])
            yield map(lambda r: ExpertRec(paper.doi, r.doi, r.score), topn[:rec_limit])

def make_classic_recs(stream, rec_limit=10):
    """Given a stream of TreeRecord, generate ClassicRecs.

    Args:
        stream: A stream of TreeRecords, sorted by cluster_id (1:1:1, 1:1:2)
        rec_limit: The number of recommendations to generate per-paper. Default 10.
        
    Returns:
        A generator returning a sorted list of ClassicRecs."""
    filtered_stream = itertools.ifilter(require_local, stream)
    classic_stream = itertools.groupby(stream, get_parent)

    for (_, stream) in classic_stream:
        candidates = [e for e in stream]
        candidates.sort(key=get_score, reverse=True)
        score = rec_limit
        for paper in candidates:
            # A paper shouldn't recommend itself
            topn = filter(lambda e: e.doi != paper.doi, candidates[:rec_limit+1])
            yield map(lambda r: ClassicRec(paper.doi, r.doi, r.score), topn[:rec_limit])

def skip_comment(fs):
    i_pos = fs.tell()
    char = fs.read(1)
    if char == '#':
        fs.readline()
        return True
    else:
        fs.seek(i_pos)
        return False

def main(infile, classic, expert, toInt=False, limit=10):
    reader = itertools.imap(lambda s: s.split(' '), infile)
    record_reader = itertools.imap(make_tree_rec, reader)
    for recs in make_expert_rec(record_reader, limit, toInt):
        score = len(recs)
        for rec in recs:
            if toInt:
                rec.score = score
                score -= 1
            expert.write(rec.to_flat())

    infile.seek(0)
    skip_comment(infile)
    reader = itertools.imap(lambda s: s.split(' '), infile)
    record_reader = itertools.imap(make_tree_rec, reader)
    for recs in make_classic_recs(record_reader, limit):
        score = len(recs)
        for rec in recs:
            if toInt:
                rec.score = score
                score -= 1
            classic.write(rec.to_flat())

#
# if __name__ == "__main__":
#     import argparse
#     import sys
#     parser = argparse.ArgumentParser()
#     parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
#     parser.add_argument('classic', type=argparse.FileType('w'))
#     parser.add_argument('expert', type=argparse.FileType('w'))
#     parser.add_argument('--toint', help="Convert scores to integers, larger is better", action='store_true', default=False)
#     parser.add_argument('-l', '--limit', type=int, help="Max number of recommendations to generate per-paper", default=10)
#     args = parser.parse_args()
#
#     skip_comment(args.infile)
#     reader = itertools.imap(lambda s: s.split(' '), args.infile)
#     record_reader = itertools.imap(make_tree_rec, reader)
#     for recs in make_expert_rec(record_reader, args.limit, args.toint):
#         score = len(recs)
#         for rec in recs:
#             if args.toint:
#                 rec.score = score
#                 score -= 1
#             args.expert.write(rec.to_flat())
#
#     args.infile.seek(0)
#     skip_comment(args.infile)
#     reader = itertools.imap(lambda s: s.split(' '), args.infile)
#     record_reader = itertools.imap(make_tree_rec, reader)
#     for recs in make_classic_recs(record_reader, args.limit):
#         score = len(recs)
#         for rec in recs:
#             if args.toint:
#                 rec.score = score
#                 score -= 1
#             args.classic.write(rec.to_flat())
