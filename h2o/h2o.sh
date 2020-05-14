h2o_start() {
  ((!$#)) && echo "h2o_start require h2o instance name as a parameter" >&2 && return 1
  echo '# h2o_start: starting h2o instance'
  nohup java -Xmx100G -Xms100G -cp ./h2o/r-h2o/h2o/java/h2o.jar water.H2OApp -name "$1" -baseport 55888 > ./h2o/log/$1.out 2> ./h2o/log/$1.err < /dev/null &
  sleep 10
}
h2o_stop() {
  echo '# h2o_stop: stopping h2o instance'
  pidof java > /dev/null 2>&1 && killall -2 java > /dev/null 2>&1
  sleep 2 && pidof java > /dev/null 2>&1 && sleep 15
  pidof java > /dev/null 2>&1 && killall -15 java > /dev/null 2>&1
  sleep 2 && pidof java > /dev/null 2>&1 && sleep 30
  pidof java > /dev/null 2>&1 && killall -9 java > /dev/null 2>&1
  sleep 2 && pidof java > /dev/null 2>&1 && sleep 60 && pidof java > /dev/null 2>&1 && echo "h2o instance could not be stopped" >&2 && return 1
  return 0
}
h2o_active() {
  pidof java > /dev/null 2>&1 && curl -X GET "localhost:55888/3/About" -H "accept: application/json" > /dev/null 2>&1
}

