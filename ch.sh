ch_start() {
  echo 'starting clickhouse-server'
  service clickhouse-server start && sleep 10
}
ch_stop() {
  echo 'stopping clickhouse-server'
  service clickhouse-server stop && sleep 10
}
ch_active() {
  clickhouse-client --query="SELECT 0;" > /dev/null 2>&1
  local ret=$?;
  if [[ $ret -eq 0 ]]; then return 0; elif [[ $ret -eq 210 ]]; then return 1; else echo "Unexpected return code from clickhouse-client: $ret" >&2 && return 1; fi;
}
