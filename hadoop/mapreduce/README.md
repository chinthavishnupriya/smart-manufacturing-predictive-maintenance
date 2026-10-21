# Hadoop MapReduce acceptance module

This Hadoop Streaming job summarizes sensor failures by machine ID.

## Data flow and concepts

1. HDFS stores the raw CSV and the job output. The NameNode manages filesystem metadata; DataNodes store blocks.
2. YARN schedules the submitted MapReduce application using the ResourceManager and NodeManagers.
3. The mapper reads each sensor CSV row and emits machine_id -> (1, failure).
4. Hadoop sorts/groups by machine_id; the reducer calculates record count, failure count, and failure-rate percent.
5. Malformed rows fail the task rather than being silently counted as valid records.

## Local contract test

From the repository root:

    python3 -m unittest discover -s tests -p 'test_hadoop_streaming.py' -v

This tests the mapper/reducer contract only; it does not prove a Hadoop cluster/YARN job ran.

## Run on a configured Hadoop cluster

Requirements: Hadoop/HDFS/YARN configured, Hadoop Streaming JAR, Python 3 on worker nodes, and the local sensor CSV.

    hadoop version
    yarn node -list
    hdfs dfs -ls /
    bash hadoop/mapreduce/run_hadoop_streaming.sh

The runner uploads data/raw/sensor_data_100k.csv to HDFS, refuses to reuse an output path, submits the Streaming job and prints the output. Set HADOOP_STREAMING_JAR to the full JAR path if it cannot be discovered. A real successful YARN job and reconciled output are required for acceptance.
