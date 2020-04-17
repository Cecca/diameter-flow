#!/bin/bash

set -e

BIN=$HOME/.cargo/bin/diameter-flow

function run_test() {
    for SEED in 112985714 524098
    do
    for DATASET in uk-2014-host # uk-2005 sk-2005
    do
        # BFS
        $BIN \
        --ddir /mnt/ssd/graphs \
        --hosts ~/working_hosts \
        --threads 4 \
        --seed $SEED \
        "bfs" \
        $DATASET

        # Hyperball
        for PARAM in 4
        do
        $BIN \
            --ddir /mnt/ssd/graphs \
            --hosts ~/working_hosts \
            --threads 4 \
            --seed $SEED \
            "hyperball($PARAM)" \
            $DATASET
        done

        # Rand cluster
        for PARAM in 4
        do
        $BIN \
            --ddir /mnt/ssd/graphs \
            --hosts ~/working_hosts \
            --threads 4 \
            --seed $SEED \
            "rand-cluster($PARAM)" \
            $DATASET
        done
    done
    done
}

case $1 in
    test)
        run_test
    ;;
    full)
        echo "To be implemented"
    ;;
esac
