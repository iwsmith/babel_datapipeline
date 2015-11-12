#!/usr/bin/env python

class EFExpert(object):
    def __init__(self, network):
        self.G = network

    def recommend(self, paper_id, n=10):
        """Return up to n recommendations for paper_id

        Args:
        paper_id: Identifier of the paper
        n: Maximum number of recommendations to return

        Returns:
        A ranked list of (paper_id, score), descending."""
        paper_id = "paper-"+paper_id
        if paper_id not in self.G:
            return []

        parent = self.G.predecessors(paper_id)
        if len(parent) != 1:
            raise "Bad state, parents != 1: " + str(len(parent))

        parent_id = parent.pop()

        children = self.G.successors(parent_id)

        recs = [(pid.split('-',1)[1], self.G.node[pid]["score"]) for pid in children if pid != paper_id]
        recs.sort(key=lambda x: x[1], reverse=True)
        return recs[:n]
