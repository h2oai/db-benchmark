# returns time left by the currently run script, useful after touch pause|stop
timeleft() {
  if [ ! -f ./run.lock ]; then
    echo "benchmark is not running now" >&2 && return 1
  fi
  Rscript -e 'source("_utils/maintainer.R"); timeleft()'
}
