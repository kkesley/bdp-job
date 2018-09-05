#!/bin/sh

pip install -r requirements.txt
chmod +x run_hadoop.sh
hadoop fs -mkdir input
hadoop fs -copyFromLocal input/* input/