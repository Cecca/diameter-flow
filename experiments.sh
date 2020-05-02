#!/bin/bash

set -e

BIN=$HOME/.cargo/bin/diameter-flow

function run_test() {
    for SEED in 112985714 524098 124098
    do
    for DATASET in uk-2014-host-lcc # uk-2005 sk-2005
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
            for BASE in 2 10
            do
                $BIN \
                    --ddir /mnt/ssd/graphs \
                    --hosts ~/working_hosts \
                    --threads 4 \
                    --seed $SEED \
                    "rand-cluster($PARAM,$BASE)" \
                    $DATASET
            done
        done
    done
    done
}

function run_web() {
    for SEED in 112985 246134 346235 2356
    do
    for DATASET in uk-2014-host-lcc uk-2005-lcc sk-2005-lcc
    do
        # BFS
        $BIN \
        --ddir /mnt/ssd/graphs \
        --hosts ~/working_hosts \
        --threads 4 \
        --seed $SEED \
        "bfs" \
        $DATASET

        # # Hyperball
        # for PARAM in 4 5 6
        # do
        # $BIN \
        #     --ddir /mnt/ssd/graphs \
        #     --hosts ~/working_hosts \
        #     --threads 4 \
        #     --seed $SEED \
        #     "hyperball($PARAM)" \
        #     $DATASET
        # done

        # Rand cluster
        for PARAM in 4 8 16
        do
            for BASE in 2 10
            do
                $BIN \
                    --ddir /mnt/ssd/graphs \
                    --hosts ~/working_hosts \
                    --threads 4 \
                    --seed $SEED \
                    "rand-cluster($PARAM,$BASE)" \
                    $DATASET
            done
        done
    done
    done
}

function run_weighted() {
    for SEED in 11985714 524098 124098
    do
    for DATASET in USA-E USA-W USA-CTR USA
    do
        # Delta-stepping
        for DELTA in 100000 1000000 10000000
        do
            $BIN \
            --ddir /mnt/ssd/graphs \
            --hosts ~/working_hosts \
            --threads 4 \
            --seed $SEED \
            "delta-stepping($DELTA)" \
            $DATASET
        done

        # Rand cluster
        for PARAM in 1000 10000 100000
        do
            for BASE in 2 10
            do
                $BIN \
                    --ddir /mnt/ssd/graphs \
                    --hosts ~/working_hosts \
                    --threads 4 \
                    --seed $SEED \
                    "rand-cluster($PARAM,$BASE)" \
                    $DATASET
            done
        done
    done
    done
}


case $1 in
    test)
        run_test
    ;;
    weighted)
        run_weighted
    ;;
    web)
        run_web
    ;;
esac
