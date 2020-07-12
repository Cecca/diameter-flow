#!/bin/bash

set -e

BIN=$HOME/.cargo/bin/diameter-flow

function run_test() {
    for SEED in 112985714 #524098 124098
    do
    for DATASET in uk-2014-host-lcc uk-2005 sk-2005
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

function run_social() {
    for SEED in 112985 #246134 346235 2356
    do
    for DATASET in orkut livejournal #friendster
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
        for PARAM in 1 2 4 8 16 32
        do
            for BASE in 2
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
    for SEED in 112985 #246134 346235 2356
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
        for PARAM in 1 2 4 8 16 32
        do
            for BASE in 2
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

function run_web_large() {
    for SEED in 112985 #246134 346235 2356
    do
    for DATASET in clueweb12
    do
        # BFS
        $BIN \
        --ddir /mnt/ssd/graphs \
        --hosts ~/working_hosts \
        --threads 4 \
        --seed $SEED \
        --offline \
        "bfs" \
        $DATASET

        # Hyperball
        for PARAM in 4 5
        do
        $BIN \
            --ddir /mnt/ssd/graphs \
            --hosts ~/working_hosts \
            --threads 4 \
            --seed $SEED \
            --offline \
            "hyperball($PARAM)" \
            $DATASET
        done

        # Rand cluster
        for PARAM in 1 2 4 8 16 32
        do
            for BASE in 2
            do
                $BIN \
                    --ddir /mnt/ssd/graphs \
                    --hosts ~/working_hosts \
                    --threads 4 \
                    --seed $SEED \
                    --offline \
                    "rand-cluster($PARAM,$BASE)" \
                    $DATASET
            done
        done
    done
    done
}


function run_weighted() {
    for SEED in 11985714 #524098 124098
    do
    for DATASET in USA-E USA-W USA-CTR USA
    do
        # Delta-stepping
        for DELTA in 100000 1000000 10000000 100000000
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
        for PARAM in 100 1000 10000 100000 1000000
        do
            for BASE in 2
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

function run_mesh() {
    for SEED in 11985714 #524098 124098
    do
    for DATASET in mesh-1000
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
        for PARAM in 8 16 64 256 1024
        do
            for BASE in 2
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

function run_rwmesh() {
    for SEED in 11985714 524098 124098
    do
    for DATASET in mesh-rw-2048
    do

        # Delta stepping
        for DELTA in 1000 10000 100000 1000000
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
        for PARAM in 100 1000 10000 100000
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

function run_scalability() {
    for SEED in 13381 #2350982 5089735 135 12346
    do
      for NUM_HOSTS in 2 4 6 8 10 12 14
      do
        cat ~/working_hosts | head -n$NUM_HOSTS > /tmp/hosts-$NUM_HOSTS
        # Rand cluster
        DATASET=uk-2005-lcc
        for PARAM in 4 # 10000
        do
            $BIN \
                --ddir /mnt/ssd/graphs \
                --hosts /tmp/hosts-$NUM_HOSTS \
                --threads 4 \
                --seed $SEED \
                "rand-cluster($PARAM,2)" \
                $DATASET
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
    social)
        run_social
    ;;
    web)
        run_web
    ;;
    web_large)
        run_web_large
    ;;
    mesh)
        run_mesh
    ;;
    rwmesh)
        run_rwmesh
    ;;
    scalability)
        run_scalability
    ;;
esac
