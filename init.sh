#!/bin/sh
yum install tmux -y
pip install -r requirements.txt
chmod +x run_hadoop.sh

aws s3 cp s3://commoncrawl/crawl-data/CC-MAIN-2018-34/wat.paths.gz input/
aws s3 cp s3://commoncrawl/crawl-data/CC-MAIN-2018-30/wat.paths.gz input-july/

gunzip input/wat.paths.gz
split -l 10000 input/wat.paths input/splitted-tenthousand-wat-
split -l 1000 input/splitted-tenthousand-wat-aa input/splitted-thousand-wat-
split -l 100 input/splitted-thousand-wat-aa input/splitted-hundred-wat-
split -l 10 input/splitted-hundred-wat-aa input/splitted-ten-wat-

gunzip input-july/wat.paths.gz
split -l 10000 input-july/wat.paths input-july/splitted-tenthousand-wat-
split -l 1000 input-july/splitted-tenthousand-wat-aa input-july/splitted-thousand-wat-
split -l 100 input-july/splitted-thousand-wat-aa input-july/splitted-hundred-wat-
split -l 10 input-july/splitted-hundred-wat-aa input-july/splitted-ten-wat-

sudo -H -u hadoop bash -c 'hadoop fs -mkdir input'
sudo -H -u hadoop bash -c 'hadoop fs -copyFromLocal input/* input'
sudo -H -u hadoop bash -c 'hadoop fs -copyFromLocal input-july/* input-july'