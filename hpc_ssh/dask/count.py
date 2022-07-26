import os
import sys
import ROOT

RDataFrame = ROOT.RDF.Experimental.Distributed.Dask.RDataFrame

from dask.distributed import Client, SSHCluster
from dask.distributed import performance_report, get_task_stream
import dask


def createSSHCluster(nodes, nprocs):
    parsed_nodes = nodes.split(',')
    scheduler = parsed_nodes[:1]
    workers = parsed_nodes[1:]
    
    print("List of nodes: scheduler ({}) and workers ({})".format(scheduler, workers))

    cluster = SSHCluster(scheduler + workers, connect_options={ "known_hosts": None }, worker_options={ "nprocs" : nprocs, "nthreads": 1, "memory_limit" : "32GB", "local_directory" : "/tmp/vpadulan" })
    client = Client(cluster)

    return cluster, client

def createDataset(num_files):
    dataset = [ 'Run2012BC_DoubleMuParked_Muons.root' ] + [ 'Run2012BC_DoubleMuParked_Muons_{}.root'.format(i) for i in range(1, num_files) ]
    print("Dataset: {}".format(dataset))
    return dataset

def run(path, dataset, npartitions, client):
    # Create dataframe from NanoAOD files
    df = RDataFrame("Events", dataset, npartitions=npartitions, daskclient=client)

    nentries = df.Count()

    print("About to run loop")
    watch = ROOT.TStopwatch()
    value = nentries.GetValue()
    elapsed = watch.RealTime()
    print("\tEvent loop dimuon_data, ncores total {}, npartitions total {}, nentries {}:".format(os.environ['ncores_total'], npartitions, value), elapsed, "s")

if __name__ == "__main__":
    print("IN MAIN")

    nodes = sys.argv[1]
    nprocs = int(sys.argv[2])
    nfiles = int(sys.argv[3])
    npartitions = int(sys.argv[4])
    ntests = int(sys.argv[5])

    cluster, client = createSSHCluster(nodes, nprocs)
    print("CLUSTER CREATED")
    dataset = createDataset(nfiles)
    path = '/tmp/vpadulan'
    files = [ os.path.join(path, data_file) for data_file in dataset ]
    #files = ["root://eospublic.cern.ch//eos/opendata/cms/derived-data/AOD2NanoAODOutreachTool/Run2012BC_DoubleMuParked_Muons.root"] * nfiles
    for i in range(ntests):
        reportpath = os.path.join(os.environ["HOME"], f"dask-report{i}.html")
        with performance_report(filename=reportpath) as pr:
            run(path, files, npartitions, client)

    client.close()
