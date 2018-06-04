# Klue Programming Challenge Roman Hartmann

The task is a bit ill-defined, but that is part of the challenge. It is a difficult problem, and typical of work at Klue. It is not necessary to work out all the details, a simple proof-of-concept implementation is what is needed, but it should work to a level of producing useful results on the data attached and the quality should be up to your coding standards. And of course to impress us a bit.

## Background
At Klue, we process a constant stream of news from all over the world. As different news outlets pick up the same story all the time, there are lots of duplicate articles. We consider two articles duplicates if they discuss the same event and describe it in similar words. Thus, two articles don’t need to be completely identical to be considered duplicates.

## Task
The idea of the challenge is to develop a small tool that filters out duplicate articles. The input is a collection of JSON files each of which contains a news article with (among others) the following three fields: a) title, b) body and c) url. The tool should read the input files and produce one output JSON file containing no duplicates. Feel free to use a programming language of your choice and avoid relying on non-standard libraries too much (but don't reinvent the wheel). Please describe your solution and motivation for your decisions. Try to focus on 1) the trade-offs you make, 2) the runtime of your solution, 3) it’s scalability, 4) limitations and future improvements. **Please don’t spend more than 8-10 hours on this challenge and cite the source in case you reuse code!**

## Bonus
Assume the dataset does not fit into memory. How can you deal with this problem, and how would you change the tool?

## Criteria
We will evaluate the solution on:
- code quality
- creativity
- quality of the deduplication
- scalability of the approach
- ability to analyse and clarify the problem and your solution in your own words
