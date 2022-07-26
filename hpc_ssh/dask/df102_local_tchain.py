import argparse
import os
import sys
from pprint import pprint
from DistRDF.Ranges import get_percentage_ranges, get_clustered_range_from_percs

import ROOT

os.environ["EXTRA_CLING_ARGS"] = ""

parser = argparse.ArgumentParser()
parser.add_argument("--nfiles", help="How many dimuon files to use", type=int)
parser.add_argument("--ntests", help="How many repetitions of the test to run", type=int)
args = parser.parse_args()

def create_dataset(nfiles):

    # Start a watch to monitor the creation of the TChain
    chainwatch = ROOT.TStopwatch()

    path = "/tmp/vpadulan"
    filenames = [f"{path}/Run2012BC_DoubleMuParked_Muons.root"] + [f"{path}/Run2012BC_DoubleMuParked_Muons_{i}.root" for i in range(1, nfiles)]
    treenames = ["Events"] * len(filenames)

    # Retrieve the task as a percentage range
    percrange = get_percentage_ranges(treenames, filenames, 1, None)[0]
    # Convert to clustered range
    clustered_range, _ = get_clustered_range_from_percs(percrange)

    # Build TEntryList for this range:
    elists = ROOT.TEntryList()
    # Build TChain of files for this range:
    chain = ROOT.TChain("Events")

    for subtreename, filename, treenentries, start, end in zip(
        clustered_range.treenames, clustered_range.filenames, clustered_range.treesnentries,
        clustered_range.localstarts, clustered_range.localends):

        # Use default constructor of TEntryList rather than the
        # constructor accepting treename and filename, otherwise
        # the TEntryList would remove any url or protocol from the
        # file name.
        elist = ROOT.TEntryList()
        elist.SetTreeName(subtreename)
        elist.SetFileName(filename)
        elist.EnterRange(start, end)
        elists.AddSubList(elist)
        chain.Add(filename + "?#" + subtreename, treenentries)

    # We assume 'end' is exclusive
    chain.SetCacheEntryRange(clustered_range.globalstart, clustered_range.globalend)

    # Connect the entry list to the chain
    chain.SetEntryList(elists, "sync")

    elapsed = chainwatch.RealTime()
    print(f"\tRuntime in creating the TChain: {elapsed} s")

    return chain

def run(dataset):
    # Create dataframe from NanoAOD files
    df = ROOT.RDataFrame(dataset)

    # For simplicity, select only events with exactly two muons and require opposite charge
    df_2mu = df.Filter("nMuon == 2", "Events with exactly two muons")
    df_os = df_2mu.Filter("Muon_charge[0] != Muon_charge[1]", "Muons with opposite charge")

    # Compute invariant mass of the dimuon system
    df_mass = df_os.Define("Dimuon_mass", "InvariantMass(Muon_pt, Muon_eta, Muon_phi, Muon_mass)")

    # Make histogram of dimuon mass spectrum
    h = df_mass.Histo1D(("Dimuon_mass", "Dimuon_mass", 30000, 0.25, 300), "Dimuon_mass")

    evwatch = ROOT.TStopwatch()
    histo = h.GetValue()
    elapsed = evwatch.RealTime()
    print(f"\tRuntime in the event loop: {elapsed} s")

    with ROOT.TFile.Open("histo_local_tchain.root", "recreate") as f:
        f.WriteObject(histo, "dimuonhisto")

if __name__ == "__main__":

    ROOT.gInterpreter.ProcessLine(".O3")
    ROOT.gROOT.SetBatch(True)
    ROOT.EnableThreadSafety()

    nfiles = args.nfiles if args.nfiles else 1
    ntests = args.ntests if args.ntests else 3
    for i in range(ntests):
        # Simulate a single DistRDF task
        # Create the dataset, create the computation graph and run the analysis
        print("Starting the analysis")
        taskwatch = ROOT.TStopwatch()
        dataset = create_dataset(nfiles)
        run(dataset)
        elapsed = taskwatch.RealTime()
        print(f"\tRuntime in the full task (tchain, {nfiles=}): {elapsed} s")

