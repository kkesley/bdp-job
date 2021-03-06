#!/bin/sh

JOB="$1"
INPUT="$2"
OUTPUT="$3"
# number of maps resp. reduces 
NUM_MAPS=4
NUM_REDUCES=10

if [ -z "$JOB" ] || [ -z "$INPUT" ] || [ -z "$OUTPUT" ]; then
    echo "Usage: $0 <job> <input> <outputdir>"
    echo "  Run a CommonCrawl mrjob on Hadoop"
    echo
    echo "Arguments:"
    echo "  <job>     CCJob implementation"
    echo "  <input>   input path"
    echo "  <output>  output path (must not exist)"
    echo
    echo "Example:"
    echo "  $0 word_count input/test-1.warc output"
    echo
    echo "Note: don't forget to adapt the number of maps/reduces and the memory requirements"
    exit 1
fi
ITERATIONS=1
SORTRANK=1
for i in "$@"
do
case $i in
    -i=*|--iterations=*)
    ITERATIONS="${i#*=}"
    shift # past argument=value
    ;;
    -s=*|--sortrank=*)
    SORTRANK="${i#*=}"
    shift # past argument=value
    ;;
    -m=*|--maps=*)
    NUM_MAPS="${i#*=}"
    shift # past argument=value
    ;;
    -r=*|--reducers=*)
    NUM_REDUCES="${i#*=}"
    shift # past argument=value
    ;;
    *)
          # unknown option
    ;;
esac
done

# strip .py from job name
JOB=${JOB%.py}

# wrap Python files for deployment, cf. below option --setup,
# see for details
# http://pythonhosted.org/mrjob/guides/setup-cookbook.html#putting-your-source-tree-in-pythonpath
tar cvfz ${JOB}_ccmr.tar.gz *.py



if [ -n "$S3_LOCAL_TEMP_DIR" ]; then
	S3_LOCAL_TEMP_DIR="--s3_local_temp_dir=$S3_LOCAL_TEMP_DIR"
else
	S3_LOCAL_TEMP_DIR=""
fi

python $JOB.py \
        --conf-path mrjob.conf \
        -r hadoop \
        --jobconf "mapreduce.map.memory.mb=768" \
        --jobconf "mapreduce.map.java.opts=-Xmx512m" \
        --jobconf "mapreduce.reduce.memory.mb=1024" \
        --jobconf "mapreduce.reduce.java.opts=-Xmx768m" \
        --jobconf "mapreduce.output.fileoutputformat.compress=true" \
        --jobconf "mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.BZip2Codec" \
        --jobconf "mapreduce.job.reduces=$NUM_REDUCES" \
        --jobconf "mapreduce.job.maps=$NUM_MAPS" \
        --setup 'export PYTHONPATH=$PYTHONPATH:'${JOB}'_ccmr.tar.gz#/' \
        --cleanup NONE \
        $S3_LOCAL_TEMP_DIR \
        --output-dir "hdfs:///user/hadoop/$OUTPUT" \
        "hdfs:///user/hadoop/$INPUT" \
        --iterations $ITERATIONS \
        --sortrank $SORTRANK

mkdir $OUTPUT
hadoop fs -copyToLocal $OUTPUT/* $OUTPUT
bzip2 -d $OUTPUT/*.bz2
cat $OUTPUT/* > $OUTPUT/output.txt
aws s3 cp $OUTPUT/output.txt s3://rmit-big-data-processing/$OUTPUT.txt
