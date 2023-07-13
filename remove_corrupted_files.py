import os
import sys
import ROOT as r
from glob import glob

check_dir = '/ceph/cms/store/user/fsetti/miniAOD_runII_20UL/GluGluToHHTo2B2G_node_cHHH1_TuneCP5_13TeV-powheg-pythia8_2017_STEP6_fixBug17/*.root'

for ifile in glob(check_dir):
	try:
		rfile = r.TFile(ifile)
		nEvents = rfile.Get("Events").GetEntries()
		rfile.Close()
	except:
		os.system("rm {}".format(ifile))
