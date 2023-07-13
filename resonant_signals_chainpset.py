import os
from metis.CMSSWTask import CMSSWTask
from metis.Sample import DirectorySample, DummySample
from metis.Path import Path
from metis.StatsParser import StatsParser
import time

import sys
sys.path.append("/home/users/fsetti/public_html/privateMC_gen")
from update_pset_NMSSM import edit_pset_graviton, edit_pset_radion
from allconfig import *

years = ['2016', '2016_APV', '2017', '2018' ]

############################################################################
############################################################################
#################     only submit 17/18 since low space on hadoop!!!!!	####
years = ['2017' ]		
############################################################################
############################################################################



condor_submit_params={
        #"sites": "SDSC-PRP", # other_sites can be good_sites, your own list, etc.
        "sites": "T2_US_UCSD,T2_US_CALTECH,T2_US_WISCONSIN,T2_US_Florida,T2_US_MIT,T2_US_Nebraska,T2_US_Purdue,T2_US_Vanderbilt", # other_sites can be good_sites, your own list, etc.
        "classads": [
            ["RequestK8SNamespace", "cms-ucsd-t2"], 
            ["SingularityImage", "/cvmfs/singularity.opensciencegrid.org/cmssw/cms:rhel7"]
        ],
        #"requirements_line": 'requirements = (HasSingularity==True)',
        "requirements_line": 'requirements = (HAS_SINGULARITY=?=True) && (TARGET.K8SNamespace =!= "abc")',
				#"use_xrootd":True
}

#condor_submit_params = {"sites" : "T2_US_UCSD,T2_US_CALTECH,T2_US_MIT,T2_US_WISCONSIN,T2_US_Nebraska,T2_US_Purdue,T2_US_Vanderbilt,T2_US_Florida",

graviton_masses = [ '250', '300', '320', '350', '400', '450', '500', '800', '1000', '2000'  ]
#graviton_masses = [ '250', '300', '320', '350', '400', '450' ]		#uaf-8
radion_masses	= [ '260', '270', '280', '290', '300', '350', '400', '500', '700', '1000' ]
#radion_masses	= [ '260']
def runall(special_dir, total_nevents, events_per_output):

	for _ in range(2500):

		proc_tag = "v0"

		#Produce resonant samples for IC people
		for proc in resonant_signals.keys() :

			if "Graviton" not in proc:
				continue

			for year in years:
				for mass in graviton_masses:

					coupling	= 'M'+str(mass)
					tag = str(proc) + "_" + coupling + '_' + year
					total_nevents_tmp = total_nevents				
					steps = []
	
					if '2016' in year:
						total_nevents_tmp /= 2
	
					try:
						cmssw_v_gen = resonant_signals[proc][year]["cmssw_v_gen"] 
						pset_gen = resonant_signals[proc][year]["pset_gen"]
						pset_gen = pset_gen.replace('_1_cfg.py','_1_cfg_%s.py'%(coupling))
						scram_arch_gen = resonant_signals[proc][year]["scram_arch_gen"]
						
						edit_pset_radion( coupling, 'M300' , year  )	
					
						step1 = CMSSWTask(
						        # Change dataset to something more meaningful (but keep STEP1, as we use this 
						        # for string replacement later); keep N=1
						        sample = DummySample(N=1, dataset="/"+ tag+ "_STEP1"),
						        # A unique identifier
						        tag = proc_tag,
						        special_dir = special_dir,
						        # Probably want to beef up the below two numbers to control splitting,
						        # but note that step2 is the bottleneck, so don't put too many events
						        # in one output file here
						        events_per_output = events_per_output,
						        total_nevents = total_nevents_tmp,
						        # We have one input dummy file, so this must be True
						        split_within_files = True,
						        pset = "cmsDrivers/UL20/"+proc+"/" + pset_gen,
						        cmssw_version = cmssw_v_gen,
						        scram_arch = scram_arch_gen,
						        condor_submit_params =  condor_submit_params
						       )
						steps += [ step1 ]
	
					except:
						print("Could not process STEP1 for: " + proc + " for " + year )
	
	
					try:
						cmssw_v_sim = resonant_signals[proc][year]["cmssw_v_sim"] 
						pset_sim = resonant_signals[proc][year]["pset_sim"]
						scram_arch_sim = resonant_signals[proc][year]["scram_arch_sim"]
						
						step2 = CMSSWTask(
						        sample = DirectorySample(
						            location = step1.get_outputdir(),
						            dataset = step1.get_sample().get_datasetname().replace("STEP1","STEP2"),
						            ),
						        tag = proc_tag,
						        special_dir = special_dir,
						        open_dataset = True,
						        files_per_output = 1,
						        pset = "cmsDrivers/UL20/"+proc+"/" + pset_sim,
						        cmssw_version = cmssw_v_sim,
						        scram_arch = scram_arch_sim,
						        condor_submit_params =  condor_submit_params
						        )
						steps += [ step2 ]
	
					except:
						print("Could not process STEP2 for: " + proc + " for " + year )
	
					try:
						cmssw_v_mix = resonant_signals[proc][year]["cmssw_v_mix"] 
						pset_mix = resonant_signals[proc][year]["pset_mix"]
						scram_arch_mix = resonant_signals[proc][year]["scram_arch_mix"]
						
						step3 = CMSSWTask(
						        sample = DirectorySample(
						            location = step2.get_outputdir(),
						            dataset = step2.get_sample().get_datasetname().replace("STEP2","STEP3"),
						            ),
						        tag = proc_tag,
						        special_dir = special_dir,
						        open_dataset = True,
						        files_per_output = 1,
						        pset = "cmsDrivers/UL20/"+proc+"/" + pset_mix,
						        cmssw_version = cmssw_v_mix,
						        scram_arch = scram_arch_mix,
						        condor_submit_params =  condor_submit_params
						        )
						steps += [ step3 ]
	
					except:
						print("Could not process STEP3 for: " + proc + " for " + year )
	
					try:
						cmssw_v_hlt = resonant_signals[proc][year]["cmssw_v_hlt"] 
						pset_hlt = resonant_signals[proc][year]["pset_hlt"]
						scram_arch_hlt = resonant_signals[proc][year]["scram_arch_hlt"]
						
						step4 = CMSSWTask(
						        sample = DirectorySample(
						            location = step3.get_outputdir(),
						            dataset = step3.get_sample().get_datasetname().replace("STEP3","STEP4"),
						            ),
						        tag = proc_tag,
						        special_dir = special_dir,
						        open_dataset = True,
						        #files_per_output = 1,
						        files_per_output = 2,
						        #output_name = "step4.root",
						        pset = "cmsDrivers/UL20/"+proc+"/" + pset_hlt,
						        cmssw_version = cmssw_v_hlt,
						        scram_arch = scram_arch_hlt,
						        condor_submit_params =  condor_submit_params
						        )
	
						steps += [ step4 ]
	
					except:
						print("Could not process STEP4 for: " + proc + " for " + year )
	
					try:
						cmssw_v_reco = resonant_signals[proc][year]["cmssw_v_reco"] 
						pset_reco = resonant_signals[proc][year]["pset_reco"]
						scram_arch_reco = resonant_signals[proc][year]["scram_arch_reco"]
						
						step5 = CMSSWTask(
						        sample = DirectorySample(
						            location = step4.get_outputdir(),
						            dataset = step4.get_sample().get_datasetname().replace("STEP4","STEP5"),
						            ),
						        tag = proc_tag,
						        special_dir = special_dir,
						        open_dataset = True,
						        #files_per_output = 1,
						        files_per_output = 2,
						        pset = "cmsDrivers/UL20/"+proc+"/" + pset_reco,
						        cmssw_version = cmssw_v_reco,
						        scram_arch = scram_arch_reco,
						        condor_submit_params =  condor_submit_params
						        )
	
						steps += [ step5 ]
	
					except:
						print("Could not process STEP5 for: " + proc + " for " + year )
	
					try:
						cmssw_v_miniaodsim = resonant_signals[proc][year]["cmssw_v_miniaodsim"] 
						pset_miniaodsim = resonant_signals[proc][year]["pset_miniaodsim"]
						scram_arch_miniaodsim = resonant_signals[proc][year]["scram_arch_miniaodsim"]
	
						step6 = CMSSWTask(
						        sample = DirectorySample(
						            location = step5.get_outputdir(),
						            dataset = step5.get_sample().get_datasetname().replace("STEP5","STEP6"),
						            ),
						        tag = proc_tag,
						        special_dir = special_dir,
						        open_dataset = True,
						        #files_per_output = 1,
						        files_per_output = 2,
						        #output_name = "step6.root",
						        pset = "cmsDrivers/UL20/"+proc+"/" + pset_miniaodsim,
						        cmssw_version = cmssw_v_miniaodsim,
						        scram_arch = scram_arch_miniaodsim,
						        condor_submit_params =  condor_submit_params
						        )
	
						steps += [ step6 ]
	
					except:
						print("Could not process STEP6 for: " + proc + " for " + year )
	
					if len(steps) == 0:
						continue
					
					total_summary = {}
					for task in steps:
					    task.process()
					    summary = task.get_task_summary()
					    total_summary[task.get_sample().get_datasetname()] = summary
					StatsParser(data=total_summary, webdir="~/public_html/dump/metis_miniAODv2/").do()     

		time.sleep( 90 * 60 )
		#time.sleep( 5 * 60 )


runall("miniAOD_runII_20UL", 400000, 250)
#runall("miniAOD_runII_20UL_test", 5, 5)
