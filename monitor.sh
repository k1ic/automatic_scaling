#!/bin/bash

while :
do
    ec_daemon=`ps -ef | grep 'python ec_scaled_daemon.py' | grep -v grep | wc -l`
    if [ $ec_daemon -eq 0 ]
    then
        source activate python37
        cd /data1/ck/scaled/src
        nohup python ec_scaled_daemon.py >> ec.log 2>&1 &
        conda deactivate
    fi

    sleep 1m
done
