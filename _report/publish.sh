#!/bin/bash
set -o errexit -o nounset

publishGhPages(){
  rm -rf db-benchmark.gh-pages
  mkdir -p db-benchmark.gh-pages
  cd db-benchmark.gh-pages

  ## Set up Repo parameters
  git init > /dev/null
  git config user.name "Tmonster"
  git config user.email "tom@ebergen.com"

  ## Set gh token from local file

  ## Reset gh-pages branch
  git remote add upstream "git@github.com:Tmonster/h2oai-db-benchmark.git"
  git fetch -q upstream gh-pages 2>err.txt
  rm -f err.txt
  git checkout -q gh-pages
  git reset -q --hard "4eadfc22cc86eade8c91f7809aae01a9753c4d90" 2>err.txt

  rm -f err.txt
  cp -r ../public/* ./
  git add -A
  git commit -q -m 'publish benchmark report' 2>err.txt
  cp ../time.csv .
  cp ../logs.csv .
  git add time.csv logs.csv 2>err.txt
  md5sum time.csv > time.csv.md5
  md5sum logs.csv > logs.csv.md5
  git add time.csv.md5 logs.csv.md5 2>err.txt
  gzip --keep time.csv
  gzip --keep logs.csv
  git add time.csv.gz logs.csv.gz 2>err.txt
  git commit -q -m 'publish benchmark timings and logs' 2>err.txt
  git push --force upstream gh-pages 2>err.txt
  
  cd ..
  
}

publishGhPages
