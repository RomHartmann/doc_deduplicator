"""Functions with algos that drive the deduplication process.

sources:
http://infolab.stanford.edu/~ullman/mmds/ch3.pdf
https://mattilyra.github.io/2017/05/23/document-deduplication-with-lsh.html
"""


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


def candidate_duplicates(document_feed, char_ngram=5, seeds=100, bands=5, hashbytes=4):
    hasher = minhash.MinHasher(seeds=seeds, char_ngram=char_ngram, hashbytes=hashbytes)
    if seeds % bands != 0:
        raise ValueError('Seeds has to be a multiple of bands. {} % {} != 0'.format(seeds, bands))

    lshcache = cache.Cache(num_bands=bands, hasher=hasher)
    for i_line, line in enumerate(document_feed):
        line = line.decode('utf8')
        docid, headline_text = line.split('\t', 1)
        fingerprint = hasher.fingerprint(headline_text.encode('utf8'))

        # in addition to storing the fingerpring store the line
        # number and document ID to help analysis later on
        lshcache.add_fingerprint(fingerprint, doc_id=(i_line, docid))

    candidate_pairs = set()
    for b in lshcache.bins:
        for bucket_id in b:
            if len(b[bucket_id]) > 1:
                pairs_ = set(itertools.combinations(b[bucket_id], r=2))
                candidate_pairs.update(pairs_)

    return candidate_pairs
