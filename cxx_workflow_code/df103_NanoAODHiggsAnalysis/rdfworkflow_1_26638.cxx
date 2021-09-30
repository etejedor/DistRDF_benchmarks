

#include "ROOT/RDataFrame.hxx"
#include "ROOT/RResultHandle.hxx"
#include "ROOT/RDFHelpers.hxx"

#include <vector>
#include <tuple>


namespace DistRDF_Internal {

using CppWorkflowResult = std::tuple<std::vector<ROOT::RDF::RResultHandle>,
                          std::vector<std::string>,
                          std::vector<ROOT::RDF::RNode>>;

CppWorkflowResult __RDF_WORKFLOW_FUNCTION__1(ROOT::RDF::RNode &rdf0)
{
  std::vector<ROOT::RDF::RResultHandle> result_handles;
  std::vector<std::string> result_types;
  std::vector<ROOT::RDF::RNode> output_nodes;

  // To make Snapshots lazy
  ROOT::RDF::RSnapshotOptions lazy_options;
  lazy_options.fLazy = true;




  auto rdf1 = rdf0.Filter("nMuon>=4", "At least four muons");
  auto rdf2 = rdf1.Filter("All(abs(Muon_pfRelIso04_all)<0.40)", "Require good isolation");
  auto rdf3 = rdf2.Filter("All(Muon_pt>5) && All(abs(Muon_eta)<2.4)", "Good muon kinematics");
  auto rdf4 = rdf3.Define("Muon_ip3d", "sqrt(Muon_dxy*Muon_dxy + Muon_dz*Muon_dz)");
  auto rdf5 = rdf4.Define("Muon_sip3d", "Muon_ip3d/sqrt(Muon_dxyErr*Muon_dxyErr + Muon_dzErr*Muon_dzErr)");
  auto rdf6 = rdf5.Filter("All(Muon_sip3d<4) && All(abs(Muon_dxy)<0.5) && All(abs(Muon_dz)<1.0)", "Track close to primary vertex with small uncertainty");
  auto rdf7 = rdf6.Filter("nMuon==4 && Sum(Muon_charge==1)==2 && Sum(Muon_charge==-1)==2", "Two positive and two negative muons");
  auto rdf8 = rdf7.Define("Z_idx", "reco_zz_to_4l(Muon_pt, Muon_eta, Muon_phi, Muon_mass, Muon_charge)");
  auto rdf9 = rdf8.Filter("filter_z_dr(Z_idx, Muon_eta, Muon_phi)", "Delta R separation of muons building Z system");
  auto rdf10 = rdf9.Define("Z_mass", "compute_z_masses_4l(Z_idx, Muon_pt, Muon_eta, Muon_phi, Muon_mass)");
  auto rdf11 = rdf10.Filter("Z_mass[0] > 40 && Z_mass[0] < 120", "Mass of first Z candidate in [40, 120]");
  auto rdf12 = rdf11.Filter("Z_mass[1] > 12 && Z_mass[1] < 120", "Mass of second Z candidate in [12, 120]");
  auto rdf13 = rdf12.Define("H_mass", "compute_higgs_mass_4l(Z_idx, Muon_pt, Muon_eta, Muon_phi, Muon_mass)");
  auto rdf14 = rdf13.Define("weight", "0.0008244082707609547");
  auto res_ptr0 = rdf14.Histo1D({"h_bkg_4mu","",36,70,180}, "H_mass", "weight");
  result_handles.emplace_back(res_ptr0);
  auto c0 = TClass::GetClass(typeid(res_ptr0));
  if (c0 == nullptr)
    throw std::runtime_error(
    "Cannot get type of result 0 of action Histo1D during "
    "generation of RDF C++ workflow");
  result_types.emplace_back(c0->GetName());

  return { result_handles, result_types, output_nodes };
}

}
