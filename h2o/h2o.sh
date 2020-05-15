java_active() {
  pgrep -U $UID java > /dev/null 2>&1
}
h2o_active() {
  java_active && curl -X GET "localhost:55888/3/About" -H "accept: application/json" > /dev/null 2>&1
}
h2o_start() {
  ((!$#)) && echo "h2o_start require h2o instance name as a parameter" >&2 && return 1
  echo '# h2o_start: starting h2o instance'
  java_active && echo "h2o instance is running already" >&2 && return 1
  nohup java -Xmx100G -Xms100G -cp ./h2o/r-h2o/h2o/java/h2o.jar water.H2OApp -name "$1" -baseport 55888 > ./h2o/log/$1.out 2> ./h2o/log/$1.err < /dev/null &
  sleep 10
}
h2o_stop() {
  echo '# h2o_stop: stopping h2o instance'
  java_active || echo "h2o instance was not running already" >&2
  java_active || return 0
  java_active && echo "sigint h2o instance" && killall -2 -u $USER java > /dev/null 2>&1
  sleep 1 && java_active && sleep 15
  java_active && echo "sigterm h2o instance" && killall -15 -u $USER java > /dev/null 2>&1
  sleep 1 && java_active && sleep 30
  java_active && echo "sigkill h2o instance" && killall -9 -u $USER java > /dev/null 2>&1
  sleep 1 && java_active && sleep 120 && java_active && echo "h2o instance could not be stopped" >&2 && return 1
  return 0
}

