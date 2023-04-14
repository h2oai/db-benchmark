ch_installed() {
  dpkg-query -Wf'${db:Status-abbrev}' clickhouse-server 2>/dev/null | grep -q '^i'
}
ch_start() {
  echo '# ch_start: starting clickhouse-server'
  sudo service clickhouse-server start && sleep 15
}
ch_stop() {
  echo '# ch_stop: stopping clickhouse-server'
  sudo service clickhouse-server stop && sleep 15
}
ch_active() {
  clickhouse-client --query="SELECT 0;" > /dev/null 2>&1
  local ret=$?;
  if [[ $ret -eq 0 ]]; then return 0; elif [[ $ret -eq 210 ]]; then return 1; else echo "Unexpected return code from clickhouse-client: $ret" >&2 && return 1; fi;
}
