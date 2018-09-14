#!/bin/sh
yum install tmux -y
pip install -r requirements.txt
chmod +x run_hadoop.sh
aws s3 cp s3://commoncrawl/crawl-data/CC-MAIN-2018-34/wat.paths.gz input/
gunzip input/wat.paths.gz
split -l 100 input/wat.paths input/splitted-hundred-wat-
split -l 1000 input/wat.paths input/splitted-thousand-wat-
split -l 10000 input/wat.paths input/splitted-tenthousand-wat-
sudo -H -u hadoop bash -c 'hadoop fs -mkdir input'
sudo -H -u hadoop bash -c 'hadoop fs -copyFromLocal input/* input'