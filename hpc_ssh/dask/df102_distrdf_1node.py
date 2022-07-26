import argparse
import os
import sys
import time

import ROOT

os.environ["EXTRA_CLING_ARGS"] = ""

RDataFrame = ROOT.RDF.Experimental.Distributed.Dask.RDataFrame

from dask.distributed import Client, SSHCluster
from dask.distributed import performance_report, get_task_stream
import dask

parser = argparse.ArgumentParser()
parser.add_argument("nodes", help="String containing the list of hostnames to be used", type=str)
parser.add_argument("nprocs", help="How many cores to use per node", type=int)
parser.add_argument("nfiles", help="How many dimuon files to use", type=int)
parser.add_argument("npartitions", help="How many partitions to use", type=int)
parser.add_argument("ntests", help="How many repetitions of the test to run", type=int)
args = parser.parse_args()

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
    return dataset

def run(path, dataset, npartitions, client):
    # Create dataframe from NanoAOD files
    df = RDataFrame("Events", dataset, npartitions=npartitions, daskclient=client)

    # For simplicity, select only events with exactly two muons and require opposite charge
    df_2mu = df.Filter("nMuon == 2", "Events with exactly two muons")
    df_os = df_2mu.Filter("Muon_charge[0] != Muon_charge[1]", "Muons with opposite charge")

    # Compute invariant mass of the dimuon system
    df_mass = df_os.Define("Dimuon_mass", "InvariantMass(Muon_pt, Muon_eta, Muon_phi, Muon_mass)")

    # Make histogram of dimuon mass spectrum
    h = df_mass.Histo1D(("Dimuon_mass", "Dimuon_mass", 30000, 0.25, 300), "Dimuon_mass")

    print("About to run loop")
    watch = ROOT.TStopwatch()
    histo = h.GetValue()
    elapsed = watch.RealTime()
    print("\n\tEvent loop dimuon_data, ncores total {}, npartitions total {}:".format(args.nprocs, npartitions), elapsed, "s")

    with ROOT.TFile.Open("histo_distrdf_1core1task.root", "recreate") as f:
        f.WriteObject(histo, "dimuonhisto")

if __name__ == "__main__":

    ROOT.gInterpreter.ProcessLine(".O3")
    ROOT.gROOT.SetBatch(True)
    ROOT.EnableThreadSafety()

    print("IN MAIN")

    cluster, client = createSSHCluster(args.nodes, args.nprocs)
    print("CLUSTER CREATED")
    
    dataset = createDataset(args.nfiles)
    path = '/tmp/vpadulan'
    files = [ os.path.join(path, data_file) for data_file in dataset ]
    #files = ["root://eospublic.cern.ch//eos/opendata/cms/derived-data/AOD2NanoAODOutreachTool/Run2012BC_DoubleMuParked_Muons.root"] * nfiles
    for i in range(args.ntests):
        run(path, files, args.npartitions, client)

    cluster.close()
    client.shutdown()
    client.close()
    time.sleep(10) # Give the cluster some time to properly stop (needed?)
