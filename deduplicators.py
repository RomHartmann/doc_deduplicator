"""Deduplicate a set of articles.

Two articles are considered duplicate if they are similarly written; i.e. plagarized.
Two articles are not considered duplicates if they discuss the same event, but written independently and uniquely.
"""
import json
import os
import datetime
import logging

from datasketch import MinHash

import algos

logging.basicConfig(level='DEBUG')
logger = logging.getLogger(__name__)


def parse_data(file_dir="news_data", max_docs=None):
    """Create a data parsing generator.

    :param file_dir: Location of the documents.
    :type file_dir: str
    :param max_docs: Cap the number of documents for testing purposes.
    :type max_docs: int or None
    :return: The important content from each document.
    :rtype: dict
    """
    if max_docs:
        filenames = os.listdir(file_dir)[0:max_docs]
    else:
        filenames = os.listdir(file_dir)

    logger.debug("analyzing {} documents".format(len(filenames)))
    for filename in filenames:
        file_path = os.path.join(file_dir, filename)
        with open(file_path, 'r') as fh:
            try:
                data = json.load(fh)
            except json.decoder.JSONDecodeError:
                logger.debug("Could not read JSON;  skipping '{}'".format(file_path))
                continue

            document_id = data.get("id", None)
            content = data.get("content", None)
            if content is None or document_id is None:
                continue

            yield {"document_id": document_id, "content": content, "filename": filename}


def time_func(func):
    """A decorator to get the execution time."""
    def timed(*args, **kw):
        begin = datetime.datetime.now()
        result = func(*args, **kw)
        elapsed = datetime.datetime.now() - begin
        logger.debug("time taken to complete = {}".format(elapsed))
        return result

    return timed


@time_func
def jaccard(file_dir="news_data", jaccard_threshold=0.75, max_docs=None):
    """Deduplicate the documents by doing a jaccard similarity score.

    NOTE:  This takes waaay too long.  Not scalable, since we do O(n^2)

    For full run on local machine:
        Number of jaccard duplicates with threshold 0.75 = 4326
        time taken to complete = 0:01:28.998940

    :param file_dir: Where to find all the documents.
    :type file_dir: str
    :param jaccard_threshold: Above this jaccard threshold we consider two documents as duplicates.
    :type jaccard_threshold: float
    :param max_docs: limit the number of docs to read for testing puposes.  None for all docs.
    :type max_docs: None or int
    :return: Each tuple is of form (doc_a_id, doc_b_id, jaccard_score)
    :rtype: list of tuples
    """
    doc_ngrams = []
    for doc in parse_data(file_dir, max_docs=max_docs):
        doc_ngrams.append((doc["filename"], algos.create_ngrams(doc["content"])))

    duplicates = []
    for i_doc in range(len(doc_ngrams)):
        for j_doc in range(i_doc + 1, len(doc_ngrams)):
            jaccard_similarity = algos.jaccard(doc_ngrams[i_doc][1], doc_ngrams[j_doc][1])
            is_duplicate = jaccard_similarity >= jaccard_threshold
            if is_duplicate:
                duplicates.append((doc_ngrams[i_doc][0], doc_ngrams[j_doc][0], jaccard_similarity))

    logger.info("Number of jaccard duplicates with threshold {} = {}".format(jaccard_threshold, len(duplicates)))

    return duplicates


@time_func
def minhash(file_dir="news_data", threshold=0.75, permutations=128):
    """Deduplicate by creating the minhash approximation of a jaccard score.

    3rd party libraries:
        https://ekzhu.github.io/datasketch/minhash.html

    For full run on local machine:
        Number of jaccard duplicates with threshold 0.75 = 4359
        time taken to complete = 0:00:08.718388

    :param file_dir: Location of all documents.
    :type file_dir: str
    :param threshold: Threshold above which we consider two documents duplicates
    :type threshold: float
    :param permutations: Number of permutations to use for the minhash
    :type permutations: int
    :return: The minhash duplicates
    :rtype: list of floats
    """
    minhashes = []
    for doc in parse_data(file_dir):
        mh = MinHash(num_perm=permutations)
        mh.update(doc["content"].encode('utf-8'))
        minhashes.append(mh)

    duplicates = []
    for i_doc in range(len(minhashes)):
        for j_doc in range(i_doc + 1, len(minhashes)):
            minhash_similarity = minhashes[i_doc].jaccard(minhashes[j_doc])

            is_duplicate = minhash_similarity >= threshold
            if is_duplicate:
                duplicates.append(minhash_similarity)

    logger.info("Number of minhash duplicates with threshold {} = {}".format(threshold, len(duplicates)))

    return duplicates


def lsh_minhash(file_dir="news_data", threshold=0.75, permutations=128):
    """Deduplicate by creating the minhash approximation of a jaccard score.

    3rd party libraries:
        https://ekzhu.github.io/datasketch/lsh.html

    For full run on local machine:
        Number of jaccard duplicates with threshold 0.75 = 4359
        time taken to complete = 0:00:08.718388

    Redis implementation:


    :param file_dir: Location of all documents.
    :type file_dir: str
    :param threshold: Threshold above which we consider two documents duplicates
    :type threshold: float
    :param permutations: Number of permutations to use for the minhash
    :type permutations: int
    :return: The minhash duplicates
    :rtype: list of floats
    """
    pass


if __name__ == '__main__':
    # jaccard(max_docs=500)
    minhash(file_dir="news_data")

# TODO
# write description
#   1) the trade-offs you make,
#   2) the runtime of your solution,
#   3) it’s scalability,
#   4) limitations and future improvements
# a) title, b) body and c) url
# log out duplicates with filename and doc_id etc.
# batch
# single new article
# find articles talking about similar event (TFIDF?)
# show strings of duplicates.  4300 dupes > 2800 documents, so there must be multiple duplicates.
# tests
