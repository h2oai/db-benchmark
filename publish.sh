#!/bin/bash
set -o errexit -o nounset

publishGhPages(){
  rm -rf db-benchmark.gh-pages
  mkdir -p db-benchmark.gh-pages
  cd db-benchmark.gh-pages

  ## Set up Repo parameters
  git init > /dev/null
  git config user.name "publish.gh-pages"
  git config user.email "publish.gh-pages@h2o.ai"

  ## Set gh token from local file
  GH_TOKEN=`cat ../token` 2>err.txt

  ## Reset gh-pages branch
  git remote add upstream "https://$GH_TOKEN@github.com/h2oai/db-benchmark.git" 2>err.txt
  git fetch -q upstream gh-pages 2>err.txt
  rm -f err.txt
  git checkout -q gh-pages
  git reset -q --hard "f2d1593f0e760f0526ce2d8759d16955f29e2c6b" 2>err.txt

  rm -f err.txt
  cp -r ../public/* ./
  git add -A
  git commit -q -m 'publish benchmark report' 2>err.txt
  cp ../time.csv .
  cp ../logs.csv .
  git add time.csv logs.csv 2>err.txt
  git commit -q -m 'publish benchmark timings and logs' 2>err.txt
  git push --force upstream gh-pages 2>err.txt
  
  cd ..
  
}

publishGhPages
