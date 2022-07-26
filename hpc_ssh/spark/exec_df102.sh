#!/bin/bash

echo "Python executable: " `which python`
echo "ROOT installation: " `which root.exe`

parsed_hostnames=`scontrol show hostnames $SLURM_JOB_NODELIST | tr '\n' ',' | sed 's/.$//'`
nodes=$((SLURM_JOB_NUM_NODES-1))
corespernode=$SLURM_CPUS_PER_TASK
memorypernode=$(($corespernode * 2))

npartitionspercore=4
ncores_total=$(( nodes * SLURM_CPUS_PER_TASK ))
npartitions=$(( ncores_total * npartitions_per_core ))
nfiles=4000
ntests=3

IFS=',' read -r -a array <<< "$parsed_hostnames"
master="${array[0]}"
workers=("${array[@]:1}")


# Copy dataset and soft link according to nfiles
for node in "${array[@]}"
do
    ssh -o StrictHostKeyChecking=no $node "if [ ! -e '/tmp/vpadulan/Run2012BC_DoubleMuParked_Muons.root' ]; then cp /hpcscratch/user/vpadulan/data/Run2012BC_DoubleMuParked_Muons.root /tmp/vpadulan; fi"
    ssh -o StrictHostKeyChecking=no $node 'for i in `seq 1 '"$(($nfiles-1))"'`; do ln -s /tmp/vpadulan/Run2012BC_DoubleMuParked_Muons.root "/tmp/vpadulan/Run2012BC_DoubleMuParked_Muons_${i}.root"; done'
    echo "COMPLETED COPIES FOR NODE $node"
done

# Start Spark master
ssh -o StrictHostKeyChecking=no $master "/hpcscratch/user/vpadulan/spark/launch-scripts/start_master.sh"
# Start Spark workers
for node in "${workers[@]}"
do
    ssh -o StrictHostKeyChecking=no $node "/hpcscratch/user/vpadulan/spark/launch-scripts/start_worker.sh $master $corespernode $memorypernode"
done
echo "Created Spark cluster"

echo "Running $ntests tests with:"
echo "- Hosts: ${parsed_hostnames}"
echo "- corespernode: $corespernode"
echo "- nfiles: $nfiles"
echo "- npartitions (total): $npartitions"
echo "- ncores (total): $(($nodes * $corespernode))"

python $PWD/df102.py $master $nodes $corespernode $nfiles $npartitions $ntests
