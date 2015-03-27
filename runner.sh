#!/bin/bash

rmids=(1,4,5,6)
gsids=(1,4)

for rid in "${rmids[@]}"
do
    echo "runs RM : $rid"
    python resourcemanager.py $rid &
done

for gid in "${gsids[@]}"
do
    echo "runs GS : $gid"
    python gridscheduler.py $gid &
done
