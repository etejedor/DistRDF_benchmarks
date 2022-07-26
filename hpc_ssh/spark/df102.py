import argparse
import os
import sys

from pyspark import SparkConf, SparkContext

import ROOT

RDataFrame = ROOT.RDF.Experimental.Distributed.Spark.RDataFrame

parser = argparse.ArgumentParser()
parser.add_argument("master", help="Hostname of the Spark master")
parser.add_argument("nodes", help="How many nodes to run with", type=int)
parser.add_argument("corespernode", help="How many cores to use on each node", type=int)
parser.add_argument("nfiles", help="How many dimuon files", type=int)
parser.add_argument("npartitions", help="How many partitions to split the dataset in", type=int)
parser.add_argument("ntests", help="How many tests to run", type=int)
args = parser.parse_args()

TOTALCORES = args.nodes * args.corespernode

def run_plain_spark(sc):
    def mapper():
        import time
        time.sleep(5)
        return 1
   
    def reducer(x, y):
        return x + y
    
    return sc.parallelize(list(range(8000)), npartitions=8000).map(mapper).treeReduce(reducer)

def createDataset(num_files):
    dataset = [ 'Run2012BC_DoubleMuParked_Muons.root' ] + [ 'Run2012BC_DoubleMuParked_Muons_{}.root'.format(i) for i in range(1, num_files) ]
    print("Dataset: {}".format(dataset))
    return dataset

def run(path, dataset, npartitions, sc):
    # Create dataframe from NanoAOD files
    files = [ os.path.join(path, data_file) for data_file in dataset ]
    df = RDataFrame("Events", files, npartitions=npartitions, sparkcontext=sc)

    # For simplicity, select only events with exactly two muons and require opposite charge
    df_2mu = df.Filter("nMuon == 2", "Events with exactly two muons")
    df_os = df_2mu.Filter("Muon_charge[0] != Muon_charge[1]", "Muons with opposite charge")

    # Compute invariant mass of the dimuon system
    df_mass = df_os.Define("Dimuon_mass", "InvariantMass(Muon_pt, Muon_eta, Muon_phi, Muon_mass)")

    # Make histogram of dimuon mass spectrum
    h = df_mass.Histo1D(("Dimuon_mass", "Dimuon_mass", 30000, 0.25, 300), "Dimuon_mass")

    # Produce plot
    ROOT.gStyle.SetOptStat(0); ROOT.gStyle.SetTextFont(42)
    c = ROOT.TCanvas("c", "", 800, 700)
    c.SetLogx(); c.SetLogy()

    print("About to run loop")
    watch = ROOT.TStopwatch()
    h.SetTitle("")
    elapsed = watch.RealTime()
    print(f"\nEvent loop dimuon_data, ncores total {TOTALCORES}, npartitions total {args.npartitions}: {elapsed} s")
    h.GetXaxis().SetTitle("m_{#mu#mu} (GeV)"); h.GetXaxis().SetTitleSize(0.04)
    h.GetYaxis().SetTitle("N_{Events}"); h.GetYaxis().SetTitleSize(0.04)
    h.Draw()

    label = ROOT.TLatex(); label.SetNDC(True)
    label.DrawLatex(0.175, 0.740, "#eta")
    label.DrawLatex(0.205, 0.775, "#rho,#omega")
    label.DrawLatex(0.270, 0.740, "#phi")
    label.DrawLatex(0.400, 0.800, "J/#psi")
    label.DrawLatex(0.415, 0.670, "#psi'")
    label.DrawLatex(0.485, 0.700, "Y(1,2,3S)")
    label.DrawLatex(0.755, 0.680, "Z")
    label.SetTextSize(0.040); label.DrawLatex(0.100, 0.920, "#bf{CMS Open Data}")
    label.SetTextSize(0.030); label.DrawLatex(0.630, 0.920, "#sqrt{s} = 8 TeV, L_{int} = 11.6 fb^{-1}")

    c.SaveAs("dimuon_spectrum.pdf")

if __name__ == "__main__":
    print("IN MAIN")

    sconf = SparkConf().setAll([("spark.master", f"spark://{args.master}:7077"),
                                ("spark.executorEnv.PYTHONPATH", os.environ["PYTHONPATH"]),
                                ("spark.executor.instances", args.nodes),
                                ("spark.executor.cores", args.corespernode),
                                ("spark.cores.max", TOTALCORES)])
    sc = SparkContext(conf=sconf)
    print("CONNECTED TO SPARK CLUSTER")
    dataset = createDataset(args.nfiles)
    path = '/tmp/vpadulan'

    for _ in range(args.ntests):
        run(path, dataset, args.npartitions, sc)

    sc.stop()
