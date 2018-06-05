"""Deduplicate a set of articles.

Two articles are considered duplicate if they are similarly written; i.e. plagarized.
Two articles are not considered duplicates if they discuss the same event, but written independently and uniquely.

These methods here are less efficient than the minhash in time.
"""
import logging

import datasketch

import common_tools

logging.basicConfig(level='DEBUG')
logger = logging.getLogger(__name__)


@common_tools.time_func
def jaccard_dedupe(data_dir="news_data", jaccard_threshold=0.75, max_docs=None):
    """Deduplicate the documents by doing a jaccard similarity score.

    NOTE:  This takes waaay too long.  Not scalable, since we do O(n^2)

    :param data_dir: Where to find all the documents.
    :type data_dir: str
    :param jaccard_threshold: Above this jaccard threshold we consider two documents as duplicates.
    :type jaccard_threshold: float
    :param max_docs: limit the number of docs to read for testing puposes.  None for all docs.
    :type max_docs: None or int
    :return: Each tuple is of form (doc_a_id, doc_b_id, jaccard_score)
    :rtype: list of tuples
    """
    doc_ngrams = []
    for doc in common_tools.parse_data(data_dir, max_docs=max_docs):
        doc_ngrams.append((doc["filename"], common_tools.create_ngrams(doc["content"])))

    duplicates = []
    for i_doc in range(len(doc_ngrams)):
        for j_doc in range(i_doc + 1, len(doc_ngrams)):
            jaccard_similarity = common_tools.jaccard(doc_ngrams[i_doc][1], doc_ngrams[j_doc][1])
            is_duplicate = jaccard_similarity >= jaccard_threshold
            if is_duplicate:
                duplicates.append((doc_ngrams[i_doc][0], doc_ngrams[j_doc][0], jaccard_similarity))

    # TODO cluster duplicates
    logger.info("Number of jaccard duplicates with threshold {} = {}".format(jaccard_threshold, len(duplicates)))

    return duplicates


@common_tools.time_func
def minhash_dedupe(data_dir="news_data", threshold=0.75, permutations=128):
    """Deduplicate by creating the minhash approximation of a jaccard score.

    3rd party libraries:
        https://ekzhu.github.io/datasketch/minhash.html

    :param data_dir: Location of all documents.
    :type data_dir: str
    :param threshold: Threshold above which we consider two documents duplicates
    :type threshold: float
    :param permutations: Number of permutations to use for the minhash
    :type permutations: int
    :return: The minhash duplicates
    :rtype: list of floats
    """
    minhashes = []
    for doc in common_tools.parse_data(data_dir):
        mh = datasketch.MinHash(num_perm=permutations)
        words = [s.encode('utf-8') for s in doc["content"].split()]
        for word in words:
            mh.update(word)
        minhashes.append(mh)

    duplicates = []
    for i_doc in range(len(minhashes)):
        for j_doc in range(i_doc + 1, len(minhashes)):
            minhash_similarity = minhashes[i_doc].jaccard(minhashes[j_doc])

            is_duplicate = minhash_similarity >= threshold
            if is_duplicate:
                duplicates.append(minhash_similarity)

    # TODO cluster duplicates
    logger.info("Number of minhash duplicates with threshold {} = {}".format(threshold, len(duplicates)))

    return duplicates


if __name__ == '__main__':
    # jaccard_dedupe(max_docs=500)
    minhash_dedupe()

# TODO
# TEST the duplicates (create visual verification platform)
# log out duplicates with filename and doc_id etc.

# TODO:
# - research into better understanding MinHash and LSH
