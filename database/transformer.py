#!/usr/bin/env python

from __future__ import print_function
from csv import DictReader, reader
from configobj import ConfigObj
from storage import Storage
from contextlib import closing
import itertools
from decimal import Decimal
from boto.exception import JSONResponseError
from tree_transform import make_tree_rec, process_tree
import uuid
import time
import logging

REC_TYPE_MAP = {"1" : "classic", "2" : "expert"}

def process_dict_stream(stream, conf):
    filtered_stream = itertools.ifilter(lambda e: e["rectype"] in REC_TYPE_MAP, stream) # Only include the rec types we know and love
    hashkey_stream = itertools.groupby(filtered_stream, lambda e: "|".join((e["targetdoi"], REC_TYPE_MAP[e["rectype"]], e["EF"])))
    for (key, stream) in hashkey_stream:
        # Boto doesn't support lists, ony sets
        recs = set([s["doi"] for s in stream])
        yield debucketer(key, recs, conf)

def make_key(e):
    return "|".join((e.target_doi, e.rec_type, str(e.score)))

def process_record_stream(stream, conf):
    for group in stream:
        hashkey_stream = itertools.groupby(group, make_key)
        for (key, stream) in hashkey_stream:
            # Boto doesn't support lists, ony sets
            recs = set([s.doi for s in stream])
            yield debucketer(key, recs, conf)

def debucketer(key, value, conf):
    (hash_key, ef) = key.rsplit("|", 1)
    # Boto's handling of float's is poor. Decimals, however, work fine.
    # See https://github.com/boto/boto/issues/2413
    return {conf["hash_key"] : hash_key,
            conf["range_key"] : Decimal(ef),
            conf["rec_attribute"] : value}

def main(publisher, filename, create=False, flush=False, dryrun=False, verbose=False, skip=False):
    import sys
    from collections import deque
    import os

    # the confspec could probably be removed
    # config = ConfigObj(infile=os.path.join(os.pardir, 'configs', 'default.cfg'),
    #                    unrepr=True, interpolation="template",
    #                    configspec=os.path.join(os.pardir,
    #                                            'configs', 'confspec.cfg')).dict()

    config = ConfigObj(infile=os.path.join(os.pardir, 'configs', 'default.cfg'),
                               unrepr=True,
                               interpolation="template",
                               configspec=os.path.join(os.pardir,
                                               'configs', 'confspec.cfg')).dict()
    print(config)

    if publisher not in PRODUCTS:
        raise ValueError('Publisher is not valid')

    with closing(Storage(config["storage"])) as c:
        table = c.tables[publisher]
        if flush:
            logging.info("Deleting table: " + table.table_name)
            try:
                if dryrun is False:
                    table.delete()
                time.sleep(20)
            except JSONResponseError as e:
                pass # Table doesn't exist, be cool.

        if create:
            logging.info("Creating table: " + table.table_name)
            if dryrun is False:
                table.create(write=2000) # Just don't forget to turn it back down
            time.sleep(20) # This call is async, so chill for a bit

        entries = 0
        start = time.time()

        if skip:
            logging.info("Skipping the first line")
            filename.next()

        reader = reader(filename, delimiter=config["metadata"][publisher]["tree"]["delimiter"])
        record_reader = itertools.imap(make_tree_rec, reader)
        entry_stream = process_record_stream(process_tree(record_reader), config["storage"][publisher])
        rate = deque(maxlen=20)
        with table.get_batch_put_context() as batch:
            for entry in entry_stream:
                if verbose:
                    print(entry)
                if dryrun is False:
                    batch.put_item(entry)
                entries += 1
                if entries % 50000 == 0:
                    current_time = time.time()
                    current_rate = entries/(current_time - start)
                    rate.append(current_rate)
                    sys.stdout.flush()
        end = time.time()
        print("\nProcessed {0:,} entries in {1:.0f} seconds: {2:.2f} entries/sec".format(entries, end-start, entries/(end-start)))

        if dryrun is False:
            table.update_throughput()

if __name__ == '__main__':
    import argparse

    PRODUCTS = ("plos", "jstor", "arxiv", "pubmed", "dblp", "ssrn", "mas", "aminer")
    parser = argparse.ArgumentParser(description="Transform reccomender output to DynamoDB")
    parser.add_argument("publisher", help="which publisher", choices=PRODUCTS)
    parser.add_argument("filename", help="file to transform", type=argparse.FileType('r'))
    parser.add_argument("-c", "--create", help="create table in database", action="store_true")
    parser.add_argument("-f", "--flush", help="flush database.", action="store_true")
    parser.add_argument("-d", "--dryrun", help="Process data, but don't insert into DB", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-s", "--skip", help="Skip first line", action="store_true")

    args = parser.parse_args()

    main(args.publisher, args.filename, args.create, args.flush, args.dryrun, args.verbose, args.skip)

