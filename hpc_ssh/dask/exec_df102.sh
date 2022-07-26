#!/bin/bash

echo "ROOT Installation: " `which root.exe`
echo "Python executable: " `which python`

parsed_hostnames=`scontrol show hostnames $SLURM_JOB_NODELIST | tr '\n' ',' | sed 's/.$//'`
nprocs=$SLURM_CPUS_PER_TASK
npartitions_per_core=1
export ncores_total=$(( (SLURM_JOB_NUM_NODES-1) * SLURM_CPUS_PER_TASK ))
npartitions=$(( ncores_total * npartitions_per_core ))
nfiles=4000
ntests=3

IFS=',' read -r -a array <<< "$parsed_hostnames"
for node in "${array[@]}"
do
    ssh -o StrictHostKeyChecking=no $node 'if [ ! -e "/tmp/vpadulan/Run2012BC_DoubleMuParked_Muons.root" ]; then cp /hpcscratch/user/vpadulan/data/Run2012BC_DoubleMuParked_Muons.root /tmp/vpadulan; fi'
    ssh -o StrictHostKeyChecking=no $node 'for i in `seq 1 3999`; do ln -s /tmp/vpadulan/Run2012BC_DoubleMuParked_Muons.root "/tmp/vpadulan/Run2012BC_DoubleMuParked_Muons_${i}.root"; done'
    echo "COMPLETED COPIES FOR NODE $node"
done

echo "Running $ntests tests with:"
echo "- Hosts: ${parsed_hostnames}"
echo "- nprocs: $nprocs"
echo "- nfiles: $nfiles"
echo "- npartitions (total): $npartitions"
echo "- ncores (total): $ncores_total"
#echo "- npartitions per core: $npartitions_per_core"

python $PWD/df102.py "${parsed_hostnames}" $nprocs $nfiles $npartitions $ntests
