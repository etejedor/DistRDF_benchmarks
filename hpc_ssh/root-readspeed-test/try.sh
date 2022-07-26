nfiles=10

declare -a filenames

for i in `seq 1 $nfiles`
do
    curfile="/tmp/vpadulan/Run2012BC_DoubleMuParked_Muons_${i}.root"
    filenames+=($curfile)
done

echo "COMPLETED COPIES FOR NODE"
echo "${filenames[@]}"
