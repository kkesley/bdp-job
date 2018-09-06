#!/bin/sh

pip install -r requirements.txt
chmod +x run_hadoop.sh
sudo -H -u hadoop bash -c 'hadoop fs -mkdir input'
sudo -H -u hadoop bash -c 'hadoop fs -copyFromLocal input/* input'