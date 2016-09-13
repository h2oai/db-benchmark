#!/bin/bash
echo Should all be near 10 Gbits/sec
rm -f ./servertest.log
ssh $USER@mr-0xd5 "nohup iperf -s > /dev/null" &
sleep 2
echo iperf server started on mr-0xd5
HOSTS="mr-0xd1 mr-0xd2 mr-0xd3 mr-0xd4 mr-0xd6 mr-0xd7 mr-0xd8 mr-0xd9 mr-0xd10"
for i in $HOSTS; do 
  (printf "$i "; ssh $USER@$i "nohup iperf -c mr-0xd5 -t 0.1 | grep Gbits | awk '{print \$8, \$9}'") >> servertest.log
done 
ssh $USER@mr-0xd5 "kill \$(ps -ef | grep '[0-9][ ]iperf[ ]-s' | awk '{print \$2}')"
cat servertest.log
echo iperf server stopped on mr-0xd5
