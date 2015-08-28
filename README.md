# Network analysis

Example code for chapter eight, [Clojure for Data Science](https://www.packtpub.com/big-data-and-business-intelligence/clojure-data-science).

## Data

This chapter makes use of Twitter data from the [Stanford Network Analysis Project](https://snap.stanford.edu/).

We use two Twitter follow graph datasets: https://snap.stanford.edu/data/twitter.tar.gz and https://snap.stanford.edu/data/twitter_combined.txt.gz.

## Instructions

### *nix and OS X

Run the following command-line script to download the data to the project's data directory:

```bash
# Downloads and unzips the data files into this project's data directory.
    
script/download-data.sh
```

### Windows / manual instructions

Download and decompress both twitter.tar.gz and twitter_combined.txt.gz to this project's data directory.

  1. Download the twitter.tar.gz file linked above to this chapter's data directory
  2. Expand the twitter.tar.gz file to a directory called twitter within the project's data directory
  3. Download and expand the twitter_combined.txt.gz file linked above to this chapter's data directory

After following the steps you should have a twitter_combined.txt file and many twitter/*.edges files inside the data directory.

## Running examples

Examples can be run with:

```bash
# Replace 8.1 with the example you want to run:

lein run -e 8.1
```
or open an interactive REPL with:

```bash
lein repl
```

## License

Copyright © 2015 Henry Garner

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
