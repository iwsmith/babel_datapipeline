#!/usr/bin/env python

class EFExpert(object):
    def __init__(self, network):
        self.G = network

    def converter(self, infile, delimeter=' '):
        import networkx as nx
        g = nx.DiGraph()
        reader = map(str.strip, list(infile))
        for pair in reader:
            indiv = pair.split(delimeter)
            print(indiv)
            g.add_edge(indiv[0], indiv[1])
        self.G = g

    def recommend(self, paper_id, n=10):
        """Return up to n recommendations for paper_id

        Args:
        paper_id: Identifier of the paper
        n: Maximum number of recommendations to return

        Returns:
        A ranked list of (paper_id, score), descending."""
        #paper_id = "paper-"+str(paper_id)
        print('\n')
        print(self.G.nodes())
        if paper_id not in self.G.nodes():
            return []

        parent = self.G.predecessors(paper_id)
        if len(parent) != 1:
            raise StandardError("Bad state, parents != 1: " + str(len(parent)))

        parent_id = parent.pop()

        children = self.G.successors(parent_id)

        recs = [(pid.split('-',1)[1], self.G.node[pid]["score"]) for pid in children if pid != paper_id]
        recs.sort(key=lambda x: x[1], reverse=True)
        return recs[:n]

    def all(self):
        #save to file
        print('this is to make it so that there are no errors')

if __name__ == "__main__":
    infile = open('citation_dict/aminer_parse_2015-11-18.txt')
    ef = EFExpert(None)
    ef.converter(infile)
    print(ef.recommend('320308'))
