#!/bin/bash

rmids=( 1 4 5 6 )
rmpid=()
gsids=( 4 8 19 )
gspid=()
for rid in "${rmids[@]}"
do
    echo "runs RM : $rid"
    python resourcemanager.py $rid &
    rmpid[$rid]=$!

    if [[ $rid = "${rmids[0]}" ]]; then
        sleep 1.5
    else
        sleep 0.5
    fi
done

for gid in "${gsids[@]}"
do
    echo "runs GS : $gid"
    python gridscheduler.py $gid &
    gspid[$gid]=$!
    if [[ $gid = "${gsids[0]}" ]]; then
        sleep 1.5
    else
        sleep 0.5
    fi
done

OPTIONS="List ListNS SpawnGS SpawnRM KillGS KillRM Quit"
select opt in $OPTIONS;
do
    if [ "$opt" = "Quit" ]; then
        echo "done"
        exit
    elif [ "$opt" = "SpawnGS" ]; then
        echo "input id GS  : "
        read idspawn
        if [[ "${gspid[$idspawn]}" = -1 ]]; then
            echo "runs GS : $idspawn"
            python gridscheduler.py $idspawn &
            gspid[$idspawn]=$!
        else
            echo "process not dead"
        fi
    elif [ "$opt" = "SpawnRM" ]; then
        echo "input id RM  : "
        read idspawn
        if [[ "${rmpid[$idspawn]}" = -1 ]]; then
            echo "runs RM : $idspawn"
            python resourcemanager.py $idspawn &
            rmpid[$idspawn]=$!
        else
            echo "process not dead"
        fi
    elif [ "$opt" = "List" ]; then
        for rid in "${rmids[@]}"
        do
            echo "RM $rid run in ${rmpid[$rid]}"
        done
        for gid in "${gsids[@]}"
        do
            echo "GS $gid run in ${gspid[$gid]}"
        done
    elif [ "$opt" = "KillGS" ]; then
        echo "input GS id  : "
        read k_gid
        kill -SIGINT ${gspid[$k_gid]}
        echo "GS killed."

        gspid[$k_gid]=-1
    elif [ "$opt" = "KillRM" ]; then
        echo "input RM id  : "
        read k_rid
        kill -SIGINT ${rmpid[$k_rid]}
        echo "RM killed."

        rmpid[$k_rid]=-1
    elif [ "$opt" = "ListNS" ]; then
        python -m Pyro4.nsc list
    fi
done
