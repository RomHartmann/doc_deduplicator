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


def parse_data(data_dir="news_data", max_docs=None):
    """Create a data parsing generator.

    :param data_dir: Location of the documents.
    :type data_dir: str
    :param max_docs: Cap the number of documents for testing purposes.
    :type max_docs: int or None
    :return: The important content from each document.
    :rtype: dict
    """
    if max_docs:
        filenames = os.listdir(data_dir)[0:max_docs]
    else:
        filenames = os.listdir(data_dir)

    logger.debug("analyzing {} documents".format(len(filenames)))
    for filename in filenames:
        file_path = os.path.join(data_dir, filename)
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


def display_duplicate_content(duplicates, data_dir="news_data", print_width=250):
    """Given duplicate filenames, show their content in order to manually check if correct.

    :param data_dir: Location of the documents.
    :type data_dir: str
    :param duplicates: A set of duplicate file names.
    :type duplicates: list of str
    :param print_width: Number of characters to display in the terminal.
    :type print_width: int or None
    :return: None
    :rtype: None
    """
    for head_dupe, sibling_dupes in duplicates:
        all_dupes = {head_dupe}.union(set(sibling_dupes))
        for dupe_name in all_dupes:
            filename = os.path.join(data_dir, dupe_name)
            with open(filename, 'r') as fd:
                data = json.load(fd)
                content = data.get("content", None)
            print_width = len(content) if print_width is None else print_width
            print(content[0:print_width])
        print("---")


def save_duplicate_filenames(duplicates, save_path="duplicates.json.nl"):
    """Given duplicate filenames, show their content in order to manually check if correct.

    File saved as newline delimited json.

    :param save_path: Location of the documents.
    :type save_path: str
    :param duplicates: A set of duplicate file names.
    :type duplicates: list of str
    :return: None
    :rtype: None
    """
    with open(save_path, 'w') as f:
        for head_dupe, sibling_dupes in duplicates:
            all_dupes = [head_dupe] + sibling_dupes
            f.write(json.dumps(all_dupes) + "\n")
    logger.info("saved duplicates to '{}'".format(save_path))


def create_ngrams(text, word_ngram=3):
    """Create a set of overlapping, full length word n-grams.

    :param text: str
    :type text: The string from which the word n-grams are created.
    :param word_ngram: int
    :type word_ngram: length of each word ngram
    :return: A set of word ngrams
    :rtype: set
    """
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
