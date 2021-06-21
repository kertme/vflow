#!/bin/bash
nohup /usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic vflow.ipfix | python3 /root/read.py > /root/readpy.log &
echo $! > /root/save_pid.txt