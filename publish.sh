#!/bin/bash
set -o errexit -o nounset

publishGhPages(){
  mkdir db-benchmark.gh-pages
  cd db-benchmark.gh-pages

  ## Set up Repo parameters
  git init
  git config user.name "publish.gh-pages"
  git config user.email "publish.gh-pages@h2o.ai"

  ## Set gh token from local file
  GH_TOKEN=`cat token` 2>err.txt

  ## Reset gh-pages branch
  git remote add upstream "https://$GH_TOKEN@github.com/h2oai/db-benchmark.git" 2>err.txt
  git fetch upstream gh-pages 2>err.txt
  git checkout gh-pages 2>err.txt
  git reset --hard "f2d1593f0e760f0526ce2d8759d16955f29e2c6b" 2>err.txt

  git add time.csv 2>err.txt
  git commit -m 'publish benchmark timings' 2>err.txt
  git add grouping.*.png 2>err.txt
  git commit -m 'publish grouping benchplots' 2>err.txt
  git push --force upstream gh-pages 2>err.txt
  
  cd ..
  rm -rf db-benchmark.gh-pages
  
}

publishGhPages
