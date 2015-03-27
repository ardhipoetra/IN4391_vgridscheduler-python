#!/bin/bash

rmids=( 1 4 5 6 )
rmpid=()
gsids=( 1 4 )
gspid=()
for rid in "${rmids[@]}"
do
    echo "runs RM : $rid"
    # python resourcemanager.py $rid &
    rmpid[$rid]=$!
done

for gid in "${gsids[@]}"
do
    echo "runs GS : $gid"
    # python gridscheduler.py $gid &
    gspid[$rid]=$!
done

OPTIONS="List kill Quit"
select opt in $OPTIONS;
do
    if [ "$opt" = "Quit" ]; then
        echo "done"
        exit
    elif [ "$opt" = "List" ]; then
        for rid in "${rmids[@]}"
        do
            echo "RM $rid run in $rmpid[$rid]"
        done
        for gid in "${gsids[@]}"
        do
            echo "GS $gid run in $gspid[$gid]"
        done
    elif [ "$opt" = "kill" ]; then
        echo "input pid  : "
        read pidkill
        kill -SIGINT $pidkill
        echo "killed."
    fi
done
