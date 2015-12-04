#!/usr/bin/env python

import boto.dynamodb2.table 
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.dynamodb2.types import NUMBER
import boto.dynamodb2
import random
import hashlib
import logging
from collections import defaultdict

def create_hash(doi, rec_type):
    return "|".join((doi, rec_type))

class Table():
    def __init__(self, connection, max_results, table_name, hash_key, range_key, rec_attribute, rec_types, product):
        self.table_name = table_name
        self.hash_key = hash_key
        self.range_key = range_key
        self.rec_attribute = rec_attribute
        self.rec_types = rec_types
        self.connection = connection
        self.max_results = max_results
        self.table = boto.dynamodb2.table.Table(table_name, connection=connection)
        self.publisher = product

    def create(self, read=5, write=5):
        return self.table.create(self.table_name,
                                 schema=[HashKey(self.hash_key),
                                         RangeKey(self.range_key,
                                                  data_type=NUMBER)],
                                 throughput={'read':read, 'write':write},
                                 connection=self.connection)
    def delete(self):
        return self.table.delete()

    def get_batch_put_context(self):
        """
        Returns the context manager to use for batched put requests.

        See http://boto.readthedocs.org/en/latest/dynamodb2_tut.html#batch-writing for details.
        """
        return self.table.batch_write()

    def put_entry(self, entry):
        return self.table.put_item(entry)

    def update_throughput(self, read=5, write=5):
        return self.table.update(throughput={'read':read, 'write':write})

    def get_rec_set(self, ids, limit=10, rec_type="classic"):
        """
        Given a set of IDs from a publisher return a dictionary of recommendations
        """
#TODO: Make this limit better, currently 100
        limit = min(limit, self.max_results*10)
        id_set = set(ids)
        uuid = hashlib.md5()
        if rec_type not in self.rec_types:
            raise ValueError("Invalid recommendation type: " + rec_type)

        #TODO: Use an ordered set so we have a stable hash
        papers = {}
        for cid in id_set:
            uuid.update(cid)
            index = create_hash(cid, rec_type)
            query_results = self.table.query(composite_doi__eq=index, limit=limit)
            for qr in query_results:
                for pid in qr[self.rec_attribute]:
                    if pid in papers and papers[pid] != qr["score"]:
                        logging.warn("{0} present already but with a different score: {1} vs {2}".format(pid, papers[pid], qr["score"]))
                        if qr["score"] > papers[pid]:
                            papers[pid] = qr["score"]
                    else:
                        papers[pid] = qr["score"]

        recs_set = defaultdict(set)
        for pid, score in papers.iteritems():
            recs_set[score].add(pid)
        recs = [(paper_ids, score) for score, paper_ids in recs_set.iteritems()]
        recs.sort(key=lambda rec: rec[1], reverse=True)

        return self._build_rec_list(recs, hashobj=uuid, limit=limit)

    def _build_rec_list(self, recs, hashobj=None, limit=10):
        results = []
        for rec, _ in recs:

            if hashobj:
                [hashobj.update(paper) for paper in rec]

            if len(rec) == 1:
                # Only one result in the score bucket, just add it
                results.extend(rec)
            else:
                # A score tie. Randomly choose from the tied elements
                # You cannot sample > len(pop)
                sample = min(len(rec), limit-len(results))
                results.extend(random.sample(rec, sample))

            if limit - len(results) <= 0:
                break

        results_dict = [{"id": paper, "publisher":self.publisher} for paper in results]
        if len(results_dict):
            return {"recommendations" : results_dict, "transaction_id" : hashobj.hexdigest()}
        else:
            return {}

    def get_recommendation(self, doi, rec_type=None, limit=100):
        """
        Given a doi, return a dictionary of recommendations.

        Returns:
            A dictionary with keys corresponding to the recommendation types
            and values are lists of recommended document DOIs, descending
            (best recommendation -> worst).

            In the event that no recommendations exist an empty dictionary
            will be returned.
        """
        results = dict()
        if rec_type is None:
            # If no type is specified look for them all
            for t in self.rec_types:
                results.update(self.get_recommendation(doi,
                                                       rec_type=t,
                                                       limit=limit))
            return results
        else:
            if rec_type not in self.rec_types:
                raise ValueError("Invalid recommendation type: " + rec_type)

        limit = min(limit, self.max_results)
        index = create_hash(doi, rec_type)
        query_results = self.table.query(composite_doi__eq=index, limit=limit)
        uuid = hashlib.md5()
        uuid.update(index)
        recs = list()
        for query_result in query_results:
            result_set = query_result[self.rec_attribute]

            for result in result_set:
                uuid.update(result)

            if len(result_set) == 1:
                # Only one result in the score bucket, just add it
                recs.extend(result_set)
            else:
                # A score tie. Randomly choose from the tied elements
                # You cannot sample > len(pop)
                sample = min(len(result_set), limit-len(recs))
                recs.extend(random.sample(result_set, sample))

            if limit - len(recs) <= 0:
                break

        recs = [{"id":x, "publisher":self.publisher} for x in recs]

        if len(recs) > 0:
            return {rec_type: {"recommendations" : recs, "transaction_id" : uuid.hexdigest()}}
        else:
            return dict()

class Storage():
    def __init__(self, storage_conf):

        self.conf = storage_conf
        self.max_results = storage_conf["max_results"]
        self.products = self.conf["products"]

        if self.conf["region"] == "localhost":
            from boto.dynamodb2.layer1 import DynamoDBConnection
            self.connection = DynamoDBConnection(
                host='localhost',
                port=8000,
                aws_secret_access_key='anything',
                is_secure=False)

        else:
            self.connection = boto.dynamodb2.connect_to_region(self.conf["region"])

        self.tables = dict()
        for prod in self.products:
            self.tables[prod] = Table(self.connection, max_results=self.max_results, **self.conf[prod])

    def close(self):
        """ Closes the connection. This allows you to use with contextlib's closing.
        Mostly necessary for the test DB which seems to only allow a single connection.
        """

        self.connection.close()

