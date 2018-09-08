#!/bin/sh
yum install tmux -y
pip install -r requirements.txt
chmod +x run_hadoop.sh
aws s3 cp s3://commoncrawl/crawl-data/CC-MAIN-2018-34/wet.paths.gz input/
gunzip input/wet.paths.gz
split -l 100 input/wet.paths input/splitted-wet-
sudo -H -u hadoop bash -c 'hadoop fs -mkdir input'
sudo -H -u hadoop bash -c 'hadoop fs -copyFromLocal input/* input'