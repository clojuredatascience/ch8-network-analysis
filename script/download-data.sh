#!/bin/bash

script_dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
data_dir="${script_dir}/../data"
twitter_url="https://snap.stanford.edu/data/twitter.tar.gz"
twitter_combined_url="https://snap.stanford.edu/data/twitter_combined.txt.gz"

mkdir -p "${data_dir}"

if [ ! -e "${data_dir}/twitter" ]; then
    echo "Downloading ${twitter_url}..."
    if [ $(curl -s --head -w %{http_code} $twitter_url -o /dev/null) -eq 200 ]; then
        download_file="${data_dir}/twitter.tar.gz"
        curl $twitter_url -o "${download_file}" 
        tar xzf "${download_file}" -C "${data_dir}"
    else
        echo "Couldn't download data. Perhaps it has moved? Consult http://wiki.clojuredatascience.com"
    fi
fi;

if [ ! -e "${data_dir}/twitter_combined.txt" ]; then
    echo "Downloading ${twitter_combined_url}..."
    if [ $(curl -s --head -w %{http_code} $twitter_combined_url -o /dev/null) -eq 200 ]; then
        download_file="${data_dir}/twitter_combined.txt.gz"
        curl $twitter_combined_url -o "${download_file}" 
        gzip -d "${download_file}"
    else
        echo "Couldn't download data. Perhaps it has moved? Consult http://wiki.clojuredatascience.com"
    fi
fi;
