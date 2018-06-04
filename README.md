# Document deduplicator

### Methodology

I went down a bit of a rabbit hole on this one.

My first idea was to implement a jaccard scoring on a word-based ngram.
Found out that it takes way too long, and I couldn't figure out an elegant pipeline that
would be able to overcome the computation time horizontally.

So then I came across this book and it's famous [chapter 3](http://infolab.stanford.edu/~ullman/mmds/ch3.pdf)
which describes how minhashing works.  That seemed tricky to implement (quickly) without big
efficiency oversights, so I found this [awesome library](https://github.com/ekzhu/datasketch)
which has a very easy to implement minhashing.  And even better, it does
[LSH + redis layer](https://ekzhu.github.io/datasketch/lsh.html)

I did not remove all of my R&D code, just because I may want to look at it again
sometime in the future.

#### The trade-offs

As far as I understand, minhash is always going to be an approximation of jaccard.
So the immediate trade-off for using any sort of hashing is accuracy for speed.


#### The runtime of solution

Jaccard:
- Number of jaccard duplicates with threshold 0.75 = 4326
- time taken to complete = 0:01:28.998940

MinHash:
- Number of jaccard duplicates with threshold 0.75 = 4359
- time taken to complete = 0:00:08.718388

LSH minhash with Redis storage layer:
- Number of jaccard duplicates with threshold 0.75 =
- time taken to complete =

#### Scalability


#### Limitations and future improvements

Limitations:

1)  Without some sort of (probably manual) checking, the accuracy is not clear.


Things to do to make production ready:

1)  Host Redis somewhere
2)  Unit tests
3)  Logging location
4)  Create a streaming API for single/groups of new documents that uses + updates existing Redis layer
5)  Use multiple dimensions: use Title, URL, date of release etc.
6)  Extract or enrich the document to gain the subject/topic of the article to link related articles.

### Implementation

1)  Install requirements with `pip install -r requirements.txt`
2)  Set up Redis (steps below)
3)  Run `python depuplicators.py`


#### Setting up local Redis

We want a redis server on localhost to act as a storage layer for the hashed docs.

