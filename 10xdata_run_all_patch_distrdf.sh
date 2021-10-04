#!/bin/bash

PYTHON=`which python3`
export PYTHONPATH=benchmarks:$PYTHONPATH

# Make sure cppyy jits at O3
export CPPYY_OPT_LEVEL=3

NPARTITIONS=1
TIMES_DIR=time_results/10xdata/patch_distrdf
NTESTS=1

mkdir -p $TIMES_DIR

for benchmark_name in df102_NanoAODDimuonAnalysis_10xdata df103_NanoAODHiggsAnalysis_10xdata df104_HiggsToTwoPhotons_10xdata
do
  for mode in "" "--optimized"
  do
    for npartitions in 1 4 8 16
    do
      echo "Running benchmark $benchmark_name with mode '$mode' and $npartitions partition(s)"
      TIME_FILENAME=$TIMES_DIR/${benchmark_name}_${mode}_${npartitions}
      for i in `seq $NTESTS`
      do
        $PYTHON launch.py --benchmark $benchmark_name --npartitions $npartitions $mode 1>> ${TIME_FILENAME}.out 2>> ${TIME_FILENAME}.err
      done
    done
  done
done

# Cleanup
rm -f rdfworkflow_*
rm *.pdf 