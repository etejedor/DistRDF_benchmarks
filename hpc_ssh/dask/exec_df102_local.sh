#!/bin/bash

echo "ROOT Installation: " `which root.exe`
echo "Python executable: " `which python`

nfiles=$RDF_JOB_NFILES
ntests=10

if [ ! -e "/tmp/vpadulan/Run2012BC_DoubleMuParked_Muons.root" ]
then
    cp /hpcscratch/user/vpadulan/data/Run2012BC_DoubleMuParked_Muons.root /tmp/vpadulan
fi

for i in `seq 1 $nfiles`
do
    ln -s /tmp/vpadulan/Run2012BC_DoubleMuParked_Muons.root "/tmp/vpadulan/Run2012BC_DoubleMuParked_Muons_${i}.root"
done

echo "COMPLETED COPIES FOR NODE"

echo "Running $ntests tests with:"
echo "- nfiles: $nfiles"
echo "- single core"

cd /hpcscratch/user/vpadulan/DistRDF_benchmarks/hpc_ssh/dask
echo "Test with rdatasetspec"
python $PWD/df102_local_rdatasetspec.py --nfiles $nfiles --ntests $ntests

echo "Test with TChain+TEntryList"
python $PWD/df102_local_tchain.py --nfiles $nfiles --ntests $ntests

echo "Test with list of filenames"
python $PWD/df102_local_filelist.py --nfiles $nfiles --ntests $ntests

