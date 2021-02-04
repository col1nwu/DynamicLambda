#!/bin/bash

if [ $# != 3 ]; then
	echo "USAGE: ./driver.sh filename n_mapper n_reducer"
	exit
fi

rm -f *.txt

# set up cgroups
sudo cgdelete -g memory:myGroup 2> /dev/null
sudo cgcreate -t $USER:$USER -a $USER:$USER -g memory:myGroup

# execute memory monitor framework
cgexec -g memory:myGroup python3 word_count.py $1 $2 $3
