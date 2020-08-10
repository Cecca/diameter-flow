#!/bin/bash

set -e

BIN=$HOME/.cargo/bin/diameter-flow

# Five hour timeout
export DIAMETER_TIMEOUT=18000

function run_social() {
    for SEED in 12587
    do
        DATASET=twitter-2010-lcc-rweight
        AVG_WEIGHT=20791197
        # Delta stepping
        for DELTA in $AVG_WEIGHT
        do
            $BIN \
            --ddir /mnt/ssd/graphs \
            --hosts ~/diameter-hosts \
            --threads 8 \
            --seed $SEED \
            "delta-stepping($DELTA)" \
            $DATASET
        done
        
        # Rand cluster
        for RADIUS in $AVG_WEIGHT $(( $AVG_WEIGHT / 2 ))
        do
              $BIN \
                  --ddir /mnt/ssd/graphs \
                  --hosts ~/diameter-hosts \
                  --threads 8 \
                  --seed $SEED \
                  "rand-cluster-guess(10000000,$RADIUS,10)" \
                  $DATASET
        done
    done
}

function run_web() {
    for SEED in 12587
    do
        DATASET=sk-2005-lcc-rweight
        AVG_WEIGHT=25033273
        # Delta stepping
        for DELTA in $AVG_WEIGHT
        do
            $BIN \
            --ddir /mnt/ssd/graphs \
            --hosts ~/diameter-hosts \
            --threads 8 \
            --seed $SEED \
            "delta-stepping($DELTA)" \
            $DATASET
        done
        
        # Rand cluster
        for RADIUS in $AVG_WEIGHT $(( $AVG_WEIGHT / 2 ))
        do
              $BIN \
                  --ddir /mnt/ssd/graphs \
                  --hosts ~/diameter-hosts \
                  --threads 8 \
                  --seed $SEED \
                  "rand-cluster-guess(10000000,$RADIUS,10)" \
                  $DATASET
        done
    done
}


function run_roads() {
    for SEED in 12587
    do
        DATASET=USA-x10
        AVG_WEIGHT=2950
        # Delta stepping
        for DELTA in $AVG_WEIGHT
        do
            $BIN \
            --ddir /mnt/ssd/graphs \
            --hosts ~/diameter-hosts \
            --threads 8 \
            --seed $SEED \
            "delta-stepping($DELTA)" \
            $DATASET
        done
        
        # Rand cluster
        for RADIUS in $AVG_WEIGHT $(( $AVG_WEIGHT / 2 ))
        do
              $BIN \
                  --ddir /mnt/ssd/graphs \
                  --hosts ~/diameter-hosts \
                  --threads 8 \
                  --seed $SEED \
                  "rand-cluster-guess(10000000,$RADIUS,10)" \
                  $DATASET
        done
    done
}


function run_scalability() {
    for SEED in 13381 2350982 5089735 135 12346
    do
      for NUM_HOSTS in 2 4 6 8 10
      do
        cat ~/diameter-hosts | head -n$NUM_HOSTS > /tmp/hosts-$NUM_HOSTS
        # Rand cluster
        DATASET=USA
        AVG_WEIGHT=2950
        $BIN \
            --ddir /mnt/ssd/graphs \
            --hosts /tmp/hosts-$NUM_HOSTS \
            --threads 8 \
            --seed $SEED \
            "rand-cluster-guess(10000000,$AVG_WEIGHT,10)" \
            $DATASET
      done
    done
}

function run_scalability_n() {
  for SEED in 13381 2350982 5089735 135 12346
  do
    for DATASET in USA USA-x5 USA-x10
    do
        $BIN \
            --ddir /mnt/ssd/graphs \
            --hosts ~/diameter-hosts \
            --threads 8 \
            --seed $SEED \
            "sequential" \
            $DATASET

        $BIN \
            --ddir /mnt/ssd/graphs \
            --hosts ~/diameter-hosts \
            --threads 8 \
            --seed $SEED \
            "delta-stepping(2950)" \
            $DATASET

        $BIN \
            --ddir /mnt/ssd/graphs \
            --hosts ~/diameter-hosts \
            --threads 4 \
            --seed $SEED \
            "rand-cluster-guess(10000000,2950,10)" \
            $DATASET
    done
  done

}

case $1 in
    roads)
        run_roads
    ;;
    social)
        run_social
    ;;
    web)
        run_web
    ;;
    scalability)
        run_scalability
    ;;
    scalability_n)
        run_scalability_n
    ;;
esac
