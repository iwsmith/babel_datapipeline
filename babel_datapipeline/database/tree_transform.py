#!/usr/bin/env python
from __future__ import print_function
import itertools
from csv import reader

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

    def __str__(self):
        return "%s\t%s\t%s\t%s" % (self.target_doi, self.rec_type, self.doi, self.score)

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

def make_expert_rec(stream, rec_limit=5):
    filtered_stream = itertools.ifilter(require_local, stream)
    expert_stream = itertools.groupby(filtered_stream, get_local)

    for (_, stream) in expert_stream:
        expert_papers = [e for e in stream]

        expert_rec = list()
        for p in expert_papers:
            count = 0
            for r in expert_papers:
                if count >= rec_limit:
                    break

                if r == p:
                    continue

                expert_rec.append(ExpertRec(p.doi, r.doi, r.score))
                count = count + 1

        #NOTE: Yielding an empty list means no recommendations
        yield expert_rec, expert_papers

def process_tree(stream, rec_limit=5):
    """Given a stream of TreeRecord, convert them to ClassicRec and ExpertRec"""
    classic_stream = itertools.groupby(stream, get_parent)
    for (classic_key, stream) in classic_stream:

        classic_recs = list()
        expert_recs = list()
        expert_papers = list()

        for (recs, papers) in make_expert_rec(stream):
            expert_papers.extend(papers)
            if recs != []:
                expert_recs.extend(recs)

        if classic_key != None:
            expert_sorted = sorted(expert_papers, key=get_score, reverse=True)
            for p in expert_sorted:
                count = 0
                for r in expert_sorted:
                    if count >= rec_limit:
                        break

                    if p == r:
                        continue

                    classic_recs.append(ClassicRec(p.doi, r.doi, r.score))
                    count = count + 1

        expert_recs.extend(classic_recs)
        yield expert_recs

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="Transform tree file to rec file")
    parser.add_argument("filename", help="file to transform", type=argparse.FileType('r'))

    args = parser.parse_args()

    reader = reader(args.filename, delimiter=' ')
    record_reader = itertools.imap(make_tree_rec, reader)
    for recs in process_tree(record_reader):
        map(print, recs)
