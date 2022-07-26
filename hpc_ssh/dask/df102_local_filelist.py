import argparse
import os
import sys

import ROOT

os.environ["EXTRA_CLING_ARGS"] = ""

parser = argparse.ArgumentParser()
parser.add_argument("--nfiles", help="How many dimuon files to use", type=int)
parser.add_argument("--ntests", help="How many repetitions of the test to run", type=int)
args = parser.parse_args()

def create_dataset(nfiles):
    dataset = [ 'Run2012BC_DoubleMuParked_Muons.root' ] + [ 'Run2012BC_DoubleMuParked_Muons_{}.root'.format(i) for i in range(1, nfiles) ]
    print("Dataset: {}".format(dataset))
    return dataset

def run(dataset):
    # Create dataframe from NanoAOD files
    df = ROOT.RDataFrame("Events", dataset)

    # For simplicity, select only events with exactly two muons and require opposite charge
    df_2mu = df.Filter("nMuon == 2", "Events with exactly two muons")
    df_os = df_2mu.Filter("Muon_charge[0] != Muon_charge[1]", "Muons with opposite charge")

    # Compute invariant mass of the dimuon system
    df_mass = df_os.Define("Dimuon_mass", "InvariantMass(Muon_pt, Muon_eta, Muon_phi, Muon_mass)")

    # Make histogram of dimuon mass spectrum
    h = df_mass.Histo1D(("Dimuon_mass", "Dimuon_mass", 30000, 0.25, 300), "Dimuon_mass")

    watch = ROOT.TStopwatch()
    histo = h.GetValue()
    elapsed = watch.RealTime()
    print(f"\tRuntime in the event loop: {elapsed} s")

    with ROOT.TFile.Open("histo_local_filelist.root", "recreate") as f:
        f.WriteObject(histo, "dimuonhisto")


if __name__ == "__main__":

    ROOT.gInterpreter.ProcessLine(".O3")
    ROOT.gROOT.SetBatch(True)
    ROOT.EnableThreadSafety()

    print("Starting the analysis")

    nfiles = args.nfiles if args.nfiles else 1
    dataset = create_dataset(nfiles)
    path = '/tmp/vpadulan'
    files = [ os.path.join(path, data_file) for data_file in dataset ]

    ntests = args.ntests if args.ntests else 3
    for i in range(ntests):
        print("Starting the analysis")
        watch = ROOT.TStopwatch()
        run(files)
        elapsed = watch.RealTime()
        print(f"\tRuntime in the full task (filelist, {nfiles=}): {elapsed} s")


