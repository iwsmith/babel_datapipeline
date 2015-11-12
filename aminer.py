#!/usr/bin/env python
import logging
from copy import deepcopy

def parse_list(line, seperator):
    result = map(str.strip, line.split(seperator))
    if len(result) == 1 and result[0] == '':
        raise ValueError
    return result

def parse_index(context, rest):
    if len(rest) == 0:
        logging.warn("No index found: " + rest)
    context["id"] = int(rest)

    return rest

def parse_title(context, rest):
    if len(rest) == 0:
        logging.info("No title found: " + rest)
    context["title"] = rest

    return rest

def parse_authors(context, rest):
    try:
        authors = parse_list(rest, ';')
        context["authors"] = authors
        return authors
    except:
        logging.info("Error parsing authors for {0}: {1}".format(context["id"], rest))
        return []

def parse_affiliations(context, rest):
    try:
        affiliations = parse_list(rest, ';')
        if len(affiliations) == 1 and affiliations[1] == '-':
            return []
        context["affiliations"] = affiliations
        return affiliations
    except:
        logging.info("Error parsing affiliations for {0}: {1}".format(context["id"], rest))
        return []

def parse_date(context, rest):
    if len(rest) == 0:
        logging.info("No date found: " + rest)
    context["date"] = rest

    return rest

def parse_venue(context, rest):
    if len(rest) == 0:
        logging.info("No venue found: " + rest)
    context["venue"] = rest

    return rest

def parse_citation(context, rest):
    if len(rest) == 0:
        logging.info("No citation found: " + rest)
    context["citations"].append(int(rest))

    return context["citations"]

def parse_abstract(context, rest):
    if len(rest) == 0:
        logging.info("No abstract found: " + rest)
    context["abstract"] = rest

    return rest

def finalize_contex(context):
    if len(context) > 1:
        return context
    return None

TOKEN_MAP = {
        "FINALIZE_CONTEXT" : finalize_contex,
        "INIT_CONTEXT" : lambda: {"citations": []},
        "#index" : parse_index,
        "#*" : parse_title,
        "#@" : parse_authors,
        "#o" : parse_affiliations,
        "#t" : parse_date,
        "#c" : parse_venue,
        "#%" : parse_citation,
        "#!" : parse_abstract
        }

class AMinerParser(object):

    def __init__(self, token_map=TOKEN_MAP):
        self.token_map = token_map
        self.init_context()
        return 

    def init_context(self):
        self.context = self.token_map["INIT_CONTEXT"]()

    def finalize_context(self):
        return self.token_map["FINALIZE_CONTEXT"](self.context)

    def parse(self, stream):
        for line in stream:
            stop = not self.parse_line(line)
            if stop and self.finalize_context():
                yield self.finalize_context()
                self.init_context()

        if self.finalize_context():
            yield self.finalize_context() 

    def parse_line(self, line):
        if line[0] == '\n':
            return False
        else:
            token, rest = line.split(' ', 1)
            self.token_map[token](self.context, rest.strip())
            return True

if __name__ == "__main__":
    import argparse
    import sys
    parser = argparse.ArgumentParser()
    # adds first argument as input file
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    # adds second argument as output file
    parser.add_argument('outfile', nargs='?', type=argparse.FileType('w'), default=sys.stdout)
    args = parser.parse_args()

    # writes a dictionary of {paper citation} to the output file
    p = AMinerParser()
    for paper in p.parse(args.infile):
        for citation in paper["citations"]:
            args.outfile.write("{0} {1}\n".format(paper["id"], citation))
