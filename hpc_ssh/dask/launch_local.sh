#!/bin/bash

ncores_per_node=32

LOGS_DIR=logs
mkdir -p $LOGS_DIR

for ncores in 32
do

for nfiles in 1 10 20 30 40 50 60 70 80 90 100
do

    # Set parameters
    nnodes=`echo "($ncores + $ncores_per_node - 1) / $ncores_per_node" | bc`

    if [ "$ncores" -ge "$ncores_per_node" ]
    then
        ncores_per_task=$ncores_per_node
    else
        ncores_per_task=$ncores
    fi

    mem=$(( 4 * ncores_per_task ))

    job_name="localrdf_test_sequential_${nfiles}files"

    time="100:00:00"

    output="$LOGS_DIR/localrdf_test_sequential_${nfiles}files_%j.out"
    error="$LOGS_DIR/localrdf_test_sequential_${nfiles}files_%j.err"

    queue=photon

    exec_script=exec_df102_local.sh

    # Add to queue 
    sbatch -p $queue --exclusive -N $nnodes -c $ncores_per_task --ntasks-per-node 1 --mem="${mem}G" --job-name $job_name --time $time --output $output --error $error --export=ALL,RDF_JOB_NFILES=$nfiles $exec_script

done # nfiles

done # ncores
