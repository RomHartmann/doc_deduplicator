"""Deduplicate a set of articles via the minhash lsh method.

https://ekzhu.github.io/datasketch/lsh.html

Two articles are considered duplicate if they are similarly written; i.e. plagarized.
Two articles are not considered duplicates if they discuss the same event, but written independently and uniquely.
"""
import logging
import pickle
import datetime

import redis
import datasketch

import common_tools

logging.basicConfig(level='DEBUG')
logger = logging.getLogger(__name__)


class LshDeduper(object):

    def __init__(self, file_dir="news_data", redis_host="localhost", redis_port=6379):
        """

        :param redis_host:
        :type redis_host:
        :param redis_port:
        :type redis_port:
        """
        self.file_dir = file_dir
        self.redis_host = redis_host
        self.redis_port = redis_port

        self.lsh = None
        self.minhashes = []

    def clear_redis_db(self):
        """Clear the redis DB.

        :return: Return self so that it can be chained.
        :rtype: LshDeduper
        """
        logger.info("clearing redis db.")
        store = redis.StrictRedis(
            host=self.redis_host,
            port=self.redis_port
        )
        store.flushdb()
        return self

    def create_lsh(self, threshold=0.75):
        """

        :param threshold:
        :type threshold:
        :return:
        :rtype:
        """
        logger.info("creating minhash lsh object with threshold = {}".format(threshold))
        self.lsh = datasketch.MinHashLSH(
            threshold=threshold,
            num_perm=128,
            storage_config={
                'type': 'redis',
                'redis': {'host': self.redis_host, 'port': self.redis_port}
            }
        )
        return self

    def build_lsh(self, permutations=128):
        """

        :param permutations:
        :type permutations:
        :return:
        :rtype:
        """
        logger.info("Building minhashes")
        for doc in common_tools.parse_data(self.file_dir):
            mh = datasketch.MinHash(num_perm=permutations)
            word_set = set([s.encode('utf-8') for s in doc["content"].split()])
            for word in word_set:
                mh.update(word)
            self.minhashes.append((doc["document_id"], mh))

        with self.lsh.insertion_session() as session:
            for key, minhash in self.minhashes:
                session.insert(key, minhash)

        return self

    def store_lsh(self, model_location="lsh_model.pkl"):
        """

        :return:
        :rtype:
        """
        if self.lsh is None:
            raise Exception("self.lsh is None.  First build_lsh or load_lsh")

        logger.info("Writing lsh model to '{}'".format(model_location))
        with open(model_location, 'wb') as f_lsh:
            pickle.dump(self.lsh, f_lsh)
        return self

    def load_lsh(self, model_location="lsh_model.pkl"):
        """

        :return:
        :rtype:
        """
        logger.info("Fetching lsh model from '{}'".format(model_location))
        with open(model_location, 'rb') as f_lsh:
            self.lsh = pickle.load(f_lsh)
        return self

    def calculate_duplicates(self):
        """Deduplicate by creating the minhash approximation of a jaccard score.

        :return: The minhash duplicates
        :rtype: list of floats
        """
        logger.debug("Calculating duplicates")
        duplicates = []
        for key, minhash in self.minhashes:
            result = self.lsh.query(minhash)
            dup = set(result) - {key}
            if dup:
                duplicates.append((key, list(dup)))

        logger.info("Number of minhash duplicates = {}".format(len(duplicates)))

        return duplicates


if __name__ == '__main__':
    begin = datetime.datetime.now()

    # To build the model and store it into redis
    # deduper = LshDeduper()
    # dupes = deduper.clear_redis_db().create_lsh().build_lsh().calculate_duplicates()
    # deduper.store_lsh()

    # To load the model from redis
    deduper = LshDeduper()
    dupes = deduper.load_lsh().build_lsh().calculate_duplicates()

    elapsed = datetime.datetime.now() - begin
    logger.debug("time taken to complete = {}".format(elapsed))
