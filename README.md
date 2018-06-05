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
sometime in the future.  The R&D code is in `rnd_deduplicators.py`

The scalable deduplicator lives in `minhash_lsh_dedupe.py`, and relied heavily on 
a third party library ([datasketch](https://ekzhu.github.io/datasketch/index.html))

#### Analysis of problem

The simplistic approach to identifying duplication between two pieces of text is
to do a jaccard index between the its ngram sets.  For larger texts, such as a news article,
it _should_ be sufficient to do word-ngrams instead of character ngrams.

The problem with this approach is that we need to calculate each document against
each other document, and so (without repetition) we have O(n^2/2) complexity.

To trade for more speed, we substitute the exact jaccard distance for an approximation,
and assume that when the data gets bigger the error gets smaller.  The way minhash does
this is by using a hashing algorithm where hashing similar values has the
same probability to clash as jaccard (by minimizing the hash algo number for each hashed set)

This however, still does not scale horizontally, so we introduce LSH, where we
hash our sets into buckets.  Though at this point I decided to stop rummaging around
the rabbit hole and start writing code, so I'm not totally clear on the maths for this.
Luckily I found a library to abstract the grimy details and got on with coding.

#### The trade-offs

As far as I understand, minhash is always going to be an approximation of jaccard.  
So the immediate trade-off for using any sort of hashing is accuracy for speed.

Also, given that this is designed to be able to run on multiple machines in parallel, 
another trade off is operational complexity.


#### The runtime of solution

Jaccard:
- Number of jaccard duplicates with threshold 0.75 = 4326
- time taken to complete = 0:01:28.998940

MinHash:
- Number of minhash duplicates with threshold 0.75 = 6202
- time taken to complete = 0:00:13.451395

LSH minhash with Redis storage layer:
- Number of unique lsh duplicate docs with threshold 0.75 = 901
- time taken to complete (new) = 0:00:09.629650
- time taken to complete (load) = 0:00:08.561414

It is important to note that the last one are unique duplicate sets.  
The first two are total number of duplicates, which includes the net of duplicates.

#### Results

I have not undergone a detailed analysis of the performance of the deduplication beyond
a simple "look at it on screen and make sure its not broken".
I'm fairly sure that a lot more can be squeezed out with simple optimisations, like playing
with the threshold value.

A simple newline delimited json file (`duplicates.json.nl`) in included in this project.

To get your own results, the program can be run as outlined in the "Implentation" section below.

#### Scalability

Given the redis layer, we can have multiple machines working in parallel, and once
they close their write stream can calculate the duplicates for the docs in their system.

It should also be noted that this implementation does not require infinite memory as is,
since the data are loaded up via a generator and huge data can be split up and hashed in
parallel.  After a parallel hashing step calculating duplicates is much faster.


#### Limitations and future improvements

Limitations:

1)  Without some sort of (probably manual) checking, the accuracy is not clear.
2)  While this centralized Redis + pickle file implementation opens up the possiblity
to run this algorithm in a MapReduce manner, some more pipelining is required to make
that happen.  I would try to use pyspark to run this, simply because it is such an elegant
MapReduce implementation.


Things to do to make production ready:

1)  Host Redis somewhere
2)  Unit tests
3)  Logging location
4)  Create a streaming API for single/groups of new documents that uses + updates existing Redis layer.
Currently it is set up so that it just reads all documents from the `news_data` folder (configurable), 
so everything is batch.  An API would really make this thing nicer.
5)  Use multiple dimensions: use Title, URL, date of release etc.
6)  Extract or enrich the document to gain the subject/topic of the article to link related articles.
7)  Put the pickle file in a stored location so that it is available everywhere.
8)  Displaying duplicate content can definitely be made better
9)  Test a good threshold value.  0.75 was arbitraraly chosen.
10)  Profile script to find inefficiencies and bottlenecks

### Implementation

1)  Install requirements with `pip install -r requirements.txt`
2)  Set up localhost Redis (steps below)
3)  Run the deduplicator:

Because of the Redis layer into which the minhashes are stored, there are two easily
available methods that are callable from command line.

We can run the deduplicator from a machine such that it saves the model into Redis
(and a pickle file required to read that model)

```
python minhash_lsh_dedupe.py new
```

Else we can load that model and find duplicates against it

```
python minhash_lsh_dedupe.py load
```

To display some text from the duplicate documents in terminal, add the `--display_dupes` flag.
eg `python minhash_lsh_dedupe.py new --display_dupes`

To save the duplicate documents to a newline delimited json file, add the `--save_dupes` flag.
eg `python minhash_lsh_dedupe.py new --save_dupes`

More info can be found by running `python minhash_lsh_dedupe.py --help`


4)  The less scalable implementations live in `rnd_deduplicators.py`, and are there mostly
for reference.  If they need to be run then their function calls can be commented/uncommented
in the `__main__` statement.

#### Setting up local Redis

We want a redis server on localhost to act as a storage layer for the hashed docs.  
[(From Digital Ocean)](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-redis)

1) `cd` to some directory and download + install.
```
wget http://download.redis.io/redis-stable.tar.gz
tar xvzf redis-stable.tar.gz
cd redis-stable
make
make test
sudo make install
```
2)  Start server on localhost:  `redis-server`

Or, configure it to run as daemon:
```
cd utils
sudo ./install_server.sh

sudo service redis_6379 start
sudo service redis_6379 stop
```