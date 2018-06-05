"""Deduplicate a set of articles via the minhash lsh method.

https://ekzhu.github.io/datasketch/lsh.html

Two articles are considered duplicate if they are similarly written; i.e. plagarized.
Two articles are not considered duplicates if they discuss the same event, but written independently and uniquely.
"""
import os
import logging
import pickle
import datetime
import argparse

import redis
import datasketch

import common_tools

logging.basicConfig(level='DEBUG')
logger = logging.getLogger(__name__)


class LshDeduper(object):

    def __init__(self, data_dir="news_data", redis_host="localhost", redis_port=6379):
        """Instantiate the deduper object.

        :param data_dir: The
        :type data_dir: str
        :param redis_host: The hostname of the redis server
        :type redis_host: str
        :param redis_port: The port number of the redis server
        :type redis_port: int
        """
        self.data_dir = data_dir
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

    def build_lsh(self, permutations=128):
        """Buld the LSH object up by injecting the minhashes into it.

        :param permutations: Number of permutations to use for minhashing.
        :type permutations: int
        :return: Returning self so that we can chain commands together.
        :rtype: LshDeduper
        """
        logger.info("Building minhashes...")
        for doc in common_tools.parse_data(self.data_dir):
            mh = datasketch.MinHash(num_perm=permutations)
            word_set = set([s.encode('utf-8') for s in doc["content"].split()])
            for word in word_set:
                mh.update(word)
            self.minhashes.append((doc["filename"], mh))

        if not self.lsh:
            raise Exception("Please first 'create_lsh' or 'load_lsh'")

        logger.debug("Inserting minhashes into lsh")
        with self.lsh.insertion_session() as session:
            for key, minhash in self.minhashes:
                session.insert(key, minhash)

        return self

    def calculate_duplicates(self):
        """Deduplicate by creating the minhash approximation of a jaccard score.

        :return: The minhash duplicates
        :rtype: list of floats
        """
        logger.debug("Calculating duplicates...")
        if not self.minhashes:
            self.build_lsh()

        duplicates = []
        for key, minhash in self.minhashes:
            result = self.lsh.query(minhash)
            dup = set(result) - {key}
            if dup:
                duplicates.append((key, list(dup)))

        logger.info("Number of minhash duplicates = {}".format(len(duplicates)))

        return duplicates

    def create_lsh(self, threshold=0.75):
        """Creates the LSH object into which the hashes are stored.

        :param threshold: The threshold over which we consider two docs a duplicate
        :type threshold: float
        :return: Returning self so that we can chain commands together.
        :rtype: LshDeduper
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

    def store_lsh(self, model_location="lsh_model.pkl"):
        """Store the LSH instructions as a pickle file in order to read model later.

        :param model_location: The location of the pickle file.
        :type model_location: str
        :return: Returning self so that we can chain commands together.
        :rtype: LshDeduper
        """
        if self.lsh is None:
            raise Exception("self.lsh is None.  First build_lsh or load_lsh")

        if os.path.isfile(model_location):
            logger.debug("removing old file")
            os.remove(model_location)

        logger.info("Writing lsh model to '{}'".format(model_location))
        with open(model_location, 'wb') as f_lsh:
            pickle.dump(self.lsh, f_lsh)
        return self

    def load_lsh(self, model_location="lsh_model.pkl"):
        """Load an existing LSH model instructions from a pickle file.  Requires model to be in redis.

        The pickle file does not contain the model, but is necessary to load the model from redis.

        :param model_location: The location of the pickle file.
        :type model_location: str
        :return: Returning self so that we can chain commands together.
        :rtype: LshDeduper
        """
        logger.info("Fetching lsh model from '{}'".format(model_location))
        with open(model_location, 'rb') as f_lsh:
            self.lsh = pickle.load(f_lsh)
        return self


if __name__ == '__main__':
    begin = datetime.datetime.now()
    parser = argparse.ArgumentParser()

    parser.add_argument(
        'mode', help="Which mode to run deduper.  new: Create new model.  load:  load existing lsh mode.",
        choices=['new', 'load']
    )
    parser.add_argument(
        '--model_path', help="That path where to save the LSH model.  Default = 'lsh_model.pkl'",
        default="lsh_model.pkl"
    )
    parser.add_argument(
        '--display_dupes', help="Include this flag if we want to display duplicate content.",
        default=False,
        action="store_true"
    )
    parser.add_argument(
        '--save_dupes', help="Include this flag if we want to save duplicate content.",
        default=False,
        action="store_true"
    )
    args = parser.parse_args()

    model_path = args.model_path

    if args.mode == "new":
        # To build the model and store it into redis
        deduper = LshDeduper()
        dupes = deduper.clear_redis_db().create_lsh().calculate_duplicates()
        deduper.store_lsh(model_path)
    elif args.mode == "load":
        # To load the model from redis
        deduper = LshDeduper()
        dupes = deduper.load_lsh(model_path).calculate_duplicates()
    else:
        raise Exception("Choose between 'new' and 'load' for mode.")

    elapsed = datetime.datetime.now() - begin
    logger.debug("time taken to complete = {}".format(elapsed))

    if args.display_dupes:
        common_tools.display_duplicate_content(dupes)

    if args.save_dupes:
        common_tools.save_duplicate_filenames(dupes)
