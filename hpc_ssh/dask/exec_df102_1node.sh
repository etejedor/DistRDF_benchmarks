#!/bin/bash

echo "ROOT Installation: " `which root.exe`
echo "Python executable: " `which python`

parsed_hostnames=`scontrol show hostnames $SLURM_JOB_NODELIST | tr '\n' ',' | sed 's/.$//'`
nfiles=50
ntests=10

IFS=',' read -r -a array <<< "$parsed_hostnames"
for node in "${array[@]}"
do
    ssh -o StrictHostKeyChecking=no $node 'if [ ! -e "/tmp/vpadulan/Run2012BC_DoubleMuParked_Muons.root" ]; then cp /hpcscratch/user/vpadulan/data/Run2012BC_DoubleMuParked_Muons.root /tmp/vpadulan; fi'
    ssh -o StrictHostKeyChecking=no $node 'for i in `seq 1 49`; do ln -s /tmp/vpadulan/Run2012BC_DoubleMuParked_Muons.root "/tmp/vpadulan/Run2012BC_DoubleMuParked_Muons_${i}.root"; done'
    echo "COMPLETED COPIES FOR NODE $node"
done


# Run first tests with RDF sequential on the same computing node
#workernode="${array[1]}"
#ssh -o StrictHostKeyChecking=no $workernode "bash $PWD/exec_df102_local.sh"

for npartitions_per_core in 1
do

for nprocs in 1
do

npartitions=$(( nprocs * npartitions_per_core ))

echo "Running $ntests tests with:"
echo "- Hosts: ${parsed_hostnames}"
echo "- nprocs: $nprocs"
echo "- nfiles: $nfiles"
echo "- npartitions (total): $npartitions"
echo "- ntests: $ntests"

python $PWD/df102_distrdf_1node.py "${parsed_hostnames}" $nprocs $nfiles $npartitions $ntests

done

done
