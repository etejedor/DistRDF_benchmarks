#include "ROOT/RDataFrame.hxx"
#include "ROOT/RDFHelpers.hxx"
#include "ROOT/RVec.hxx"
#include "ROOT/RDF/RInterface.hxx"
#include "TCanvas.h"
#include "TH1D.h"
#include "TLatex.h"
#include "TLegend.h"
#include <Math/Vector4Dfwd.h>
#include <Math/GenVector/LorentzVector.h>
#include <Math/GenVector/PtEtaPhiM4D.h>
#include "TStyle.h"
#include <string>
#include <vector>
#include <TStopwatch.h>
#include <ROOT/RLogger.hxx>

auto verbosity = ROOT::Experimental::RLogScopedVerbosity(ROOT::Detail::RDF::RDFLogChannel(), ROOT::Experimental::ELogLevel::kInfo);

using namespace ROOT::VecOps;
using RNode = ROOT::RDF::RNode;
using rvec_f = const RVec<float> &;
using rvec_i = const RVec<int> &;

const auto z_mass = 91.2;

// Select interesting events with four muons
RNode selection_4mu(RNode df)
{
   auto df_ge4m = df.Filter([](unsigned int nMuon) { return nMuon >= 4; }, {"nMuon"},  "At least four muons");
   auto df_iso = df_ge4m.Filter([](rvec_f Muon_pfRelIso04_all) { return All(abs(Muon_pfRelIso04_all)<0.40); }, {"Muon_pfRelIso04_all"}, "Require good isolation");
   auto df_kin = df_iso.Filter([](rvec_f Muon_pt, rvec_f Muon_eta) { return All(Muon_pt>5) && All(abs(Muon_eta)<2.4); }, {"Muon_pt", "Muon_eta"}, "Good muon kinematics");
   auto df_ip3d = df_kin.Define("Muon_ip3d", [](rvec_f Muon_dxy, rvec_f Muon_dz) { return sqrt(Muon_dxy*Muon_dxy + Muon_dz*Muon_dz); }, { "Muon_dxy", "Muon_dz"});
   auto df_sip3d = df_ip3d.Define("Muon_sip3d", [](rvec_f Muon_ip3d, rvec_f Muon_dxyErr, rvec_f Muon_dzErr) { return Muon_ip3d/sqrt(Muon_dxyErr*Muon_dxyErr + Muon_dzErr*Muon_dzErr);} , {"Muon_ip3d", "Muon_dxyErr", "Muon_dzErr"});
   auto df_pv = df_sip3d.Filter([](rvec_f Muon_sip3d, rvec_f Muon_dxy, rvec_f Muon_dz) { return All(Muon_sip3d<4) && All(abs(Muon_dxy)<0.5) && All(abs(Muon_dz)<1.0); }, {"Muon_sip3d", "Muon_dxy", "Muon_dz"}, "Track close to primary vertex with small uncertainty");
   auto df_2p2n = df_pv.Filter([](unsigned int nMuon, rvec_i Muon_charge) { return nMuon==4 && Sum(Muon_charge==1)==2 && Sum(Muon_charge==-1)==2; }, {"nMuon", "Muon_charge"}, "Two positive and two negative muons");
   return df_2p2n;
}

// Select interesting events with four electrons
RNode selection_4el(RNode df)
{
   auto df_ge4el = df.Filter([](unsigned int nElectron) { return nElectron>=4; }, {"nElectron"}, "At least four electrons");
   auto df_iso = df_ge4el.Filter([](rvec_f Electron_pfRelIso03_all) { return All(abs(Electron_pfRelIso03_all)<0.40); }, {"Electron_pfRelIso03_all"}, "Require good isolation");
   auto df_kin = df_iso.Filter([](rvec_f Electron_pt, rvec_f Electron_eta) { return All(Electron_pt>7) && All(abs(Electron_eta)<2.5); }, {"Electron_pt", "Electron_eta"} , "Good Electron kinematics");
   auto df_ip3d = df_kin.Define("Electron_ip3d", [](rvec_f Electron_dxy, rvec_f Electron_dz) { return sqrt(Electron_dxy*Electron_dxy + Electron_dz*Electron_dz); }, {"Electron_dxy", "Electron_dz"});
   auto df_sip3d = df_ip3d.Define("Electron_sip3d", [](rvec_f Electron_ip3d, rvec_f Electron_dxyErr, rvec_f Electron_dzErr) { return Electron_ip3d/sqrt(Electron_dxyErr*Electron_dxyErr + Electron_dzErr*Electron_dzErr); }, {"Electron_ip3d", "Electron_dxyErr", "Electron_dzErr"});
   auto df_pv = df_sip3d.Filter([](rvec_f Electron_sip3d, rvec_f Electron_dxy, rvec_f Electron_dz) { return All(Electron_sip3d<4) && All(abs(Electron_dxy)<0.5) && All(abs(Electron_dz)<1.0); }, {"Electron_sip3d", "Electron_dxy", "Electron_dz"}, "Track close to primary vertex with small uncertainty");
   auto df_2p2n = df_pv.Filter([](unsigned int nElectron, rvec_i Electron_charge) { return nElectron==4 && Sum(Electron_charge==1)==2 && Sum(Electron_charge==-1)==2; }, {"nElectron", "Electron_charge"} , "Two positive and two negative electrons");
   return df_2p2n;
}

// Reconstruct two Z candidates from four leptons of the same kind
RVec<RVec<size_t>> reco_zz_to_4l(rvec_f pt, rvec_f eta, rvec_f phi, rvec_f mass, rvec_i charge)
{
   RVec<RVec<size_t>> idx(2);
   idx[0].reserve(2); idx[1].reserve(2);

   // Find first lepton pair with invariant mass closest to Z mass
   auto idx_cmb = Combinations(pt, 2);
   auto best_mass = -1;
   size_t best_i1 = 0; size_t best_i2 = 0;
   for (size_t i = 0; i < idx_cmb[0].size(); i++) {
      const auto i1 = idx_cmb[0][i];
      const auto i2 = idx_cmb[1][i];
      if (charge[i1] != charge[i2]) {
         ROOT::Math::PtEtaPhiMVector p1(pt[i1], eta[i1], phi[i1], mass[i1]);
         ROOT::Math::PtEtaPhiMVector p2(pt[i2], eta[i2], phi[i2], mass[i2]);
         const auto this_mass = (p1 + p2).M();
         if (std::abs(z_mass - this_mass) < std::abs(z_mass - best_mass)) {
            best_mass = this_mass;
            best_i1 = i1;
            best_i2 = i2;
         }
      }
   }
   idx[0].emplace_back(best_i1);
   idx[0].emplace_back(best_i2);

   // Reconstruct second Z from remaining lepton pair
   for (size_t i = 0; i < 4; i++) {
      if (i != best_i1 && i != best_i2) {
         idx[1].emplace_back(i);
      }
   }

   // Return indices of the pairs building two Z bosons
   return idx;
}

// Compute Z masses from four leptons of the same kind and sort ascending in distance to Z mass
RVec<float> compute_z_masses_4l(const RVec<RVec<size_t>> &idx, rvec_f pt, rvec_f eta, rvec_f phi, rvec_f mass)
{
   RVec<float> z_masses(2);
   for (size_t i = 0; i < 2; i++) {
      const auto i1 = idx[i][0]; const auto i2 = idx[i][1];
      ROOT::Math::PtEtaPhiMVector p1(pt[i1], eta[i1], phi[i1], mass[i1]);
      ROOT::Math::PtEtaPhiMVector p2(pt[i2], eta[i2], phi[i2], mass[i2]);
      z_masses[i] = (p1 + p2).M();
   }
   if (std::abs(z_masses[0] - z_mass) < std::abs(z_masses[1] - z_mass)) {
      return z_masses;
   } else {
      return Reverse(z_masses);
   }
}

// Compute mass of Higgs from four leptons of the same kind
float compute_higgs_mass_4l(const RVec<RVec<size_t>> &idx, rvec_f pt, rvec_f eta, rvec_f phi, rvec_f mass)
{
   const auto i1 = idx[0][0]; const auto i2 = idx[0][1];
   const auto i3 = idx[1][0]; const auto i4 = idx[1][1];
   ROOT::Math::PtEtaPhiMVector p1(pt[i1], eta[i1], phi[i1], mass[i1]);
   ROOT::Math::PtEtaPhiMVector p2(pt[i2], eta[i2], phi[i2], mass[i2]);
   ROOT::Math::PtEtaPhiMVector p3(pt[i3], eta[i3], phi[i3], mass[i3]);
   ROOT::Math::PtEtaPhiMVector p4(pt[i4], eta[i4], phi[i4], mass[i4]);
   return (p1 + p2 + p3 + p4).M();
}

// Apply selection on reconstructed Z candidates
RNode filter_z_candidates(RNode df)
{
   auto df_z1_cut = df.Filter([](rvec_f Z_mass) { return Z_mass[0] > 40 && Z_mass[0] < 120; }, {"Z_mass"}, "Mass of first Z candidate in [40, 120]");
   auto df_z2_cut = df_z1_cut.Filter([](rvec_f Z_mass) { return Z_mass[1] > 12 && Z_mass[1] < 120; }, {"Z_mass"}, "Mass of second Z candidate in [12, 120]");
   return df_z2_cut;
}

// Reconstruct Higgs from four muons
RNode reco_higgs_to_4mu(RNode df)
{
   // Filter interesting events
   auto df_base = selection_4mu(df);

   // Reconstruct Z systems
   auto df_z_idx =
      df_base.Define("Z_idx", reco_zz_to_4l, {"Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass", "Muon_charge"});

   // Cut on distance between muons building Z systems
   auto filter_z_dr = [](const RVec<RVec<size_t>> &idx, rvec_f eta, rvec_f phi) {
      for (size_t i = 0; i < 2; i++) {
         const auto i1 = idx[i][0];
         const auto i2 = idx[i][1];
         const auto dr = DeltaR(eta[i1], eta[i2], phi[i1], phi[i2]);
         if (dr < 0.02) {
            return false;
         }
      }
      return true;
   };
   auto df_z_dr =
      df_z_idx.Filter(filter_z_dr, {"Z_idx", "Muon_eta", "Muon_phi"}, "Delta R separation of muons building Z system");

   // Compute masses of Z systems
   auto df_z_mass =
      df_z_dr.Define("Z_mass", compute_z_masses_4l, {"Z_idx", "Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass"});

   // Cut on mass of Z candidates
   auto df_z_cut = filter_z_candidates(df_z_mass);

   // Reconstruct H mass
   auto df_h_mass =
      df_z_cut.Define("H_mass", compute_higgs_mass_4l, {"Z_idx", "Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass"});

   return df_h_mass;
}

// Reconstruct Higgs from four electrons
RNode reco_higgs_to_4el(RNode df)
{
   // Filter interesting events
   auto df_base = selection_4el(df);

   // Reconstruct Z systems
   auto df_z_idx = df_base.Define("Z_idx", reco_zz_to_4l,
                                  {"Electron_pt", "Electron_eta", "Electron_phi", "Electron_mass", "Electron_charge"});

   // Cut on distance between Electrons building Z systems
   auto filter_z_dr = [](const RVec<RVec<size_t>> &idx, rvec_f eta, rvec_f phi) {
      for (size_t i = 0; i < 2; i++) {
         const auto i1 = idx[i][0];
         const auto i2 = idx[i][1];
         const auto dr = DeltaR(eta[i1], eta[i2], phi[i1], phi[i2]);
         if (dr < 0.02) {
            return false;
         }
      }
      return true;
   };
   auto df_z_dr = df_z_idx.Filter(filter_z_dr, {"Z_idx", "Electron_eta", "Electron_phi"},
                                  "Delta R separation of Electrons building Z system");

   // Compute masses of Z systems
   auto df_z_mass = df_z_dr.Define("Z_mass", compute_z_masses_4l,
                                   {"Z_idx", "Electron_pt", "Electron_eta", "Electron_phi", "Electron_mass"});

   // Cut on mass of Z candidates
   auto df_z_cut = filter_z_candidates(df_z_mass);

   // Reconstruct H mass
   auto df_h_mass = df_z_cut.Define("H_mass", compute_higgs_mass_4l,
                                    {"Z_idx", "Electron_pt", "Electron_eta", "Electron_phi", "Electron_mass"});

   return df_h_mass;
}

// Plot invariant mass for signal and background processes from simulated events
// overlay the measured data.
void plot(TH1D &h_sig, TH1D &h_bkg, TH1D &h_data, const std::string &x_label, const std::string &filename)
{
   // Canvas and general style options
   gStyle->SetOptStat(0);
   gStyle->SetTextFont(42);
   auto c = new TCanvas("c", "", 800, 700);
   c->SetLeftMargin(0.15);

   // Get signal and background histograms and stack them to show Higgs signal
   // on top of the background process
   TH1D h_cmb{h_sig};
   h_cmb.Add(&h_bkg);
   h_cmb.SetTitle("");
   h_cmb.GetXaxis()->SetTitle(x_label.c_str());
   h_cmb.GetXaxis()->SetTitleSize(0.04);
   h_cmb.GetYaxis()->SetTitle("N_{Events}");
   h_cmb.GetYaxis()->SetTitleSize(0.04);
   h_cmb.SetLineColor(kRed);
   h_cmb.SetLineWidth(2);
   h_cmb.SetMaximum(18);

   h_bkg.SetLineWidth(2);
   h_bkg.SetFillStyle(1001);
   h_bkg.SetLineColor(kBlack);
   h_bkg.SetFillColor(kAzure - 9);


   h_data.SetLineWidth(1);
   h_data.SetMarkerStyle(20);
   h_data.SetMarkerSize(1.0);
   h_data.SetMarkerColor(kBlack);
   h_data.SetLineColor(kBlack);


   // Draw histograms
   h_cmb.DrawClone("HIST");
   h_bkg.DrawClone("HIST SAME");
   h_data.DrawClone("PE1 SAME");

   // Add legend
   TLegend legend(0.62, 0.70, 0.82, 0.88);
   legend.SetFillColor(0);
   legend.SetBorderSize(0);
   legend.SetTextSize(0.03);
   legend.AddEntry(&h_data, "Data", "PE1");
   legend.AddEntry(&h_bkg, "ZZ", "f");
   legend.AddEntry(&h_cmb, "m_{H} = 125 GeV", "f");
   legend.Draw();

   // Add header
   TLatex cms_label;
   cms_label.SetTextSize(0.04);
   cms_label.DrawLatexNDC(0.16, 0.92, "#bf{CMS Open Data}");
   TLatex header;
   header.SetTextSize(0.03);
   header.DrawLatexNDC(0.63, 0.92, "#sqrt{s} = 8 TeV, L_{int} = 11.6 fb^{-1}");

   // Save plot
   c->SaveAs(filename.c_str());
}

void run()
{
   std::string path = "data";

   // Create dataframes for signal, background and data samples

   // Signal: Higgs -> 4 leptons
   std::vector<std::string> df_sig_4l_paths(10, path + "/SMHiggsToZZTo4L.root");
   ROOT::RDataFrame df_sig_4l("Events", df_sig_4l_paths);

   // Background: ZZ -> 4 leptons
   // Note that additional background processes from the original paper with minor contribution were left out for this
   // tutorial.
   std::vector<std::string> df_bkg_4mu_paths(10, path + "/ZZTo4mu.root");
   std::vector<std::string> df_bkg_4el_paths(10, path + "/ZZTo4e.root");
   ROOT::RDataFrame df_bkg_4mu("Events", df_bkg_4mu_paths);
   ROOT::RDataFrame df_bkg_4el("Events", df_bkg_4el_paths);

   // CMS data taken in 2012 (11.6 fb^-1 integrated luminosity)
   std::vector<std::string> df_data_doublemu_paths;
   df_data_doublemu_paths.reserve(10);
   std::vector<std::string> df_data_doubleel_paths;
   df_data_doubleel_paths.reserve(10);
   const std::string path_doublemu_b{path + "/Run2012B_DoubleMuParked.root"};
   const std::string path_doublemu_c{path + "/Run2012C_DoubleMuParked.root"};
   const std::string path_doubleel_b{path + "/Run2012B_DoubleElectron.root"};
   const std::string path_doubleel_c{path + "/Run2012C_DoubleElectron.root"};
   for(int i = 0; i < 5; i++){
      df_data_doublemu_paths.push_back(path_doublemu_b);
      df_data_doublemu_paths.push_back(path_doublemu_c);
      df_data_doubleel_paths.push_back(path_doubleel_b);
      df_data_doubleel_paths.push_back(path_doubleel_c);
   }
   ROOT::RDataFrame df_data_doublemu("Events", df_data_doublemu_paths);
   ROOT::RDataFrame df_data_doubleel("Events", df_data_doubleel_paths);

   // Reconstruct Higgs to 4 muons
   auto df_sig_4mu_reco = reco_higgs_to_4mu(df_sig_4l);
   const auto luminosity = 11580.0;            // Integrated luminosity of the data samples
   const auto xsec_SMHiggsToZZTo4L = 0.0065;   // H->4l: Standard Model cross-section
   const auto nevt_SMHiggsToZZTo4L = 299973.0; // H->4l: Number of simulated events
   const auto nbins = 36;                      // Number of bins for the invariant mass spectrum
   auto df_h_sig_4mu = df_sig_4mu_reco
         .Define("weight", [&]() { return luminosity * xsec_SMHiggsToZZTo4L / nevt_SMHiggsToZZTo4L; }, {})
         .Histo1D<float, double>({"h_sig_4mu", "", nbins, 70, 180}, "H_mass", "weight");
   const auto scale_ZZTo4l = 1.386;     // ZZ->4mu: Scale factor for ZZ to four leptons
   const auto xsec_ZZTo4mu = 0.077;     // ZZ->4mu: Standard Model cross-section
   const auto nevt_ZZTo4mu = 1499064.0; // ZZ->4mu: Number of simulated events
   auto df_bkg_4mu_reco = reco_higgs_to_4mu(df_bkg_4mu);
   auto df_h_bkg_4mu = df_bkg_4mu_reco
         .Define("weight", [&]() { return luminosity * xsec_ZZTo4mu * scale_ZZTo4l / nevt_ZZTo4mu; }, {})
         .Histo1D<float, double>({"h_bkg_4mu", "", nbins, 70, 180}, "H_mass", "weight");

   auto df_data_4mu_reco = reco_higgs_to_4mu(df_data_doublemu);
   auto df_h_data_4mu = df_data_4mu_reco
         .Define("weight", []() { return 1.0; }, {})
         .Histo1D<float, double>({"h_data_4mu", "", nbins, 70, 180}, "H_mass", "weight");

   // Reconstruct Higgs to 4 electrons
   auto df_sig_4el_reco = reco_higgs_to_4el(df_sig_4l);
   auto df_h_sig_4el = df_sig_4el_reco
         .Define("weight", [&]() { return luminosity * xsec_SMHiggsToZZTo4L / nevt_SMHiggsToZZTo4L; }, {})
         .Histo1D<float, double>({"h_sig_4el", "", nbins, 70, 180}, "H_mass", "weight");

   const auto xsec_ZZTo4el = xsec_ZZTo4mu; // ZZ->4el: Standard Model cross-section
   const auto nevt_ZZTo4el = 1499093.0;    // ZZ->4el: Number of simulated events
   auto df_bkg_4el_reco = reco_higgs_to_4el(df_bkg_4el);
   auto df_h_bkg_4el = df_bkg_4el_reco
         .Define("weight", [&]() { return luminosity * xsec_ZZTo4el * scale_ZZTo4l / nevt_ZZTo4el; }, {})
         .Histo1D<float, double>({"h_bkg_4el", "", nbins, 70, 180}, "H_mass", "weight");

   auto df_data_4el_reco = reco_higgs_to_4el(df_data_doubleel);
   auto df_h_data_4el = df_data_4el_reco.Define("weight", []() { return 1.0; }, {})
                           .Histo1D<float, double>({"h_data_4el", "", nbins, 70, 180}, "H_mass", "weight");

   // Trigger event loops and retrieve histograms
   TStopwatch watch;
   auto signal_4mu = df_h_sig_4mu.GetValue();
   double elapsed_sig_4mu{watch.RealTime()};
   watch.Start();
   auto background_4mu = df_h_bkg_4mu.GetValue();
   double elapsed_bkg_4mu{watch.RealTime()};
   watch.Start();
   auto data_4mu = df_h_data_4mu.GetValue();
   double elapsed_data_4mu{watch.RealTime()};

   watch.Start();
   auto signal_4el = df_h_sig_4el.GetValue();
   double elapsed_sig_4el{watch.RealTime()};
   watch.Start();
   auto background_4el = df_h_bkg_4el.GetValue();
   double elapsed_bkg_4el{watch.RealTime()};
   watch.Start();
   auto data_4el = df_h_data_4el.GetValue();
   double elapsed_data_4el{watch.RealTime()};

   std::cout << "Event loop df_sig_4l: " << std::fixed << std::setprecision(2) << elapsed_sig_4mu << " s\n";
   std::cout << "Event loop df_bkg_4mu: " << std::fixed << std::setprecision(2) << elapsed_bkg_4mu << " s\n";
   std::cout << "Event loop df_data_doublemu: " << std::fixed << std::setprecision(2) << elapsed_data_4mu << " s\n";
   std::cout << "Event loop df_bkg_4el: " << std::fixed << std::setprecision(2) << elapsed_bkg_4el << " s\n";
   std::cout << "Event loop df_data_doubleel: " << std::fixed << std::setprecision(2) << elapsed_data_4el << " s\n";

   // Produce histograms for different channels and make plots
   plot(signal_4mu, background_4mu, data_4mu, "m_{4#mu} (GeV)", "higgs_4mu.pdf");
   plot(signal_4el, background_4el, data_4el, "m_{4e} (GeV)", "higgs_4el.pdf");

}

int main() {
   run(); 
}
