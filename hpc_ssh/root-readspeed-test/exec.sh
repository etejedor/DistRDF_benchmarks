#!/bin/bash

source /hpcscratch/user/vpadulan/.bashrc

nfiles=1

ntests=3

if [ ! -e "/tmp/vpadulan/Run2012BC_DoubleMuParked_Muons.root" ]
then
    cp /hpcscratch/user/vpadulan/data/Run2012BC_DoubleMuParked_Muons.root /tmp/vpadulan
fi

declare -a filenames

for i in `seq 1 $nfiles`
do
    curfile="/tmp/vpadulan/Run2012BC_DoubleMuParked_Muons_${i}.root"
    ln -s /tmp/vpadulan/Run2012BC_DoubleMuParked_Muons.root $curfile
    filenames+=($curfile)
done

echo "COMPLETED COPIES FOR NODE"
echo "${filenames[@]}"

echo "Running $ntests tests with:"
echo "- nfiles: $nfiles"
echo "- root-readspeed"

for i in `seq 1 $ntests`
do
    root-readspeed --trees Events --files "${filenames[@]}" --branches nMuon Muon_charge Muon_pt Muon_eta Muon_phi Muon_mass
done
