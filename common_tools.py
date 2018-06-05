"""Functions with algos that drive the deduplication process.

sources:
http://infolab.stanford.edu/~ullman/mmds/ch3.pdf
https://mattilyra.github.io/2017/05/23/document-deduplication-with-lsh.html
"""
import json
import os
import logging
import datetime

logging.basicConfig(level='DEBUG')
logger = logging.getLogger(__name__)


def time_func(func):
    """A decorator to get the execution time."""
    def timed(*args, **kw):
        begin = datetime.datetime.now()
        result = func(*args, **kw)
        elapsed = datetime.datetime.now() - begin
        logger.debug("time taken to complete = {}".format(elapsed))
        return result

    return timed


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
                logger.debug("Could not parse JSON;  skipping '{}'".format(file_path))
                continue

            document_id = data.get("id", None)
            content = data.get("content", None)
            if content is None or document_id is None:
                continue

            clean_content = content.lower().strip()
            yield {"document_id": document_id, "content": clean_content, "filename": filename}


def create_ngrams(text, word_ngram=3):
    """Create a set of overlapping, full length word n-grams.

    :param text: str
    :type text: The string from which the word n-grams are created.
    :param word_ngram: int
    :type word_ngram: length of each word ngram
    :return: A set of word ngrams
    :rtype: set
    """
    # TODO test optimisation on two dimensions (length of ngram), (word vs character ngram)
    words = text.lower().split()
    ngrams = [words[pos:pos + word_ngram] for pos in range(0, len(words) - word_ngram)]
    ngrams_set = set([' '.join(g) for g in ngrams])
    return ngrams_set


def jaccard(set_a, set_b):
    """Jaccard similarity of two sets.

    :param set_a: set
    :type set_a: Some set
    :param set_b: set
    :type set_b: Another set.
    :return: Jaccard similarity score
    :rtype: float
    """
    intersection = set_a.intersection(set_b)
    union = set_a.union(set_b)
    sim = 0 if len(union) == 0 else len(intersection) / len(union)
    return sim
