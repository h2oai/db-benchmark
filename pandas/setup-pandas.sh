#!/bin/bash
set -e

# Required only on client machine

# install python, pandas and pydoop.hdfs
apt-get update
apt-get install build-essential python-dev python-pip
sudo pip install --upgrade pandas
pip install pydoop.hdfs

# check
python
import pandas as pd
pd.__version__

# export ALL_HOSTS="mr-0xd1 mr-0xd2 mr-0xd3 mr-0xd4 mr-0xd5 mr-0xd7 mr-0xd8 mr-0xd9 mr-0xd10"
# python2 -c 'import pandas as pd; print pd.__version__'
# python3 -c 'import pandas as pd; print pd.__version__'
# for i in $ALL_HOSTS; do cmd="ssh $USER@$i 'python2 -c \"import pandas as pd; print pd.__version__\"'"; echo $cmd; eval $cmd; done
# for i in $ALL_HOSTS; do cmd="ssh $USER@$i 'python3 -c \"import pandas as pd; print pd.__version__\"'"; echo $cmd; eval $cmd; done

# sudo pip install --upgrade pandas
# sudo pip3 install pandas
