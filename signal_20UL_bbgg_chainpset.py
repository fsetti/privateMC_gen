import os
from metis.CMSSWTask import CMSSWTask
from metis.Sample import DirectorySample, DummySample
from metis.Path import Path
from metis.StatsParser import StatsParser
import time

import sys
sys.path.append("/home/users/fsetti/public_html/privateMC_gen")
from update_pset_NMSSM import edit_pset_bbgg
from allconfig import signal_UL20

#years = ['2016', '2016_APV', '2017', '2018' ]

############################################################################
############################################################################
#################     only submit 17/18 since low space on hadoop!!!!!	####
years = ['2017' ]		
############################################################################
############################################################################



condor_submit_params={
        #"sites": "SDSC-PRP", # other_sites can be good_sites, your own list, etc.
        "sites": "T2_US_UCSD,T2_US_CALTECH,T2_US_WISCONSIN,T2_US_Florida,T2_US_MIT,T2_US_Purdue", # other_sites can be good_sites, your own list, etc.
        "classads": [
            ["RequestK8SNamespace", "cms-ucsd-t2"], 
            ["SingularityImage", "/cvmfs/singularity.opensciencegrid.org/cmssw/cms:rhel7"]
        ],
        #"requirements_line": 'requirements = (HasSingularity==True)',
        "requirements_line": 'requirements = (HAS_SINGULARITY=?=True) && (TARGET.K8SNamespace =!= "abc")',
				#"use_xrootd":True
}

#condor_submit_params = {"sites" : "T2_US_UCSD,T2_US_CALTECH,T2_US_MIT,T2_US_WISCONSIN,T2_US_Nebraska,T2_US_Purdue,T2_US_Vanderbilt,T2_US_Florida",

couplings = [ 'cHHH1' ]

def runall(special_dir, total_nevents, events_per_output):

	proc_tag = "fixBug17"

	for _ in range(2500):

		for coupling in couplings:

			for year in years:
			
				cmssw_v_gen = signal_UL20[year]["cmssw_v_gen"] 
				pset_gen = signal_UL20[year]["pset_gen"]
				pset_gen = pset_gen.replace('_1_cfg.py','_1_cfg_%s.py'%(coupling))
				scram_arch_gen = signal_UL20[year]["scram_arch_gen"]
				
				cmssw_v_sim = signal_UL20[year]["cmssw_v_sim"] 
				pset_sim = signal_UL20[year]["pset_sim"]
				scram_arch_sim = signal_UL20[year]["scram_arch_sim"]
				
				cmssw_v_mix = signal_UL20[year]["cmssw_v_mix"] 
				pset_mix = signal_UL20[year]["pset_mix"]
				scram_arch_mix = signal_UL20[year]["scram_arch_mix"]
				
				cmssw_v_hlt = signal_UL20[year]["cmssw_v_hlt"] 
				pset_hlt = signal_UL20[year]["pset_hlt"]
				scram_arch_hlt = signal_UL20[year]["scram_arch_hlt"]
				
				cmssw_v_reco = signal_UL20[year]["cmssw_v_reco"] 
				pset_reco = signal_UL20[year]["pset_reco"]
				scram_arch_reco = signal_UL20[year]["scram_arch_reco"]
				
				cmssw_v_miniaodsim = signal_UL20[year]["cmssw_v_miniaodsim"] 
				pset_miniaodsim = signal_UL20[year]["pset_miniaodsim"]
				scram_arch_miniaodsim = signal_UL20[year]["scram_arch_miniaodsim"]
				
				
				tag = 'GluGluToHHTo2B2G_node_' + coupling + '_TuneCP5_13TeV-powheg-pythia8_' + year
				
				edit_pset_bbgg( coupling, 'cHHH1' , year  )	
				
				total_nevents_tmp = total_nevents
				
				if '2016' in year:
					total_nevents_tmp /= 2

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
				        pset = "cmsDrivers/UL20/HHTo2B2G/" + pset_gen,
				        cmssw_version = cmssw_v_gen,
				        scram_arch = scram_arch_gen,
				        condor_submit_params =  condor_submit_params
				       )
				
				step2 = CMSSWTask(
				        sample = DirectorySample(
				            location = step1.get_outputdir(),
				            dataset = step1.get_sample().get_datasetname().replace("STEP1","STEP2"),
				            ),
				        tag = proc_tag,
				        special_dir = special_dir,
				        open_dataset = True,
				        files_per_output = 1,
				        pset = "cmsDrivers/UL20/HHTo2B2G/" + pset_sim,
				        cmssw_version = cmssw_v_sim,
				        scram_arch = scram_arch_sim,
				        condor_submit_params =  condor_submit_params
				        )
				
				step3 = CMSSWTask(
				        sample = DirectorySample(
				            location = step2.get_outputdir(),
				            dataset = step2.get_sample().get_datasetname().replace("STEP2","STEP3"),
				            ),
				        tag = proc_tag,
				        special_dir = special_dir,
				        open_dataset = True,
				        files_per_output = 1,
				        pset = "cmsDrivers/UL20/HHTo2B2G/" + pset_mix,
				        cmssw_version = cmssw_v_mix,
				        scram_arch = scram_arch_mix,
				        condor_submit_params =  condor_submit_params
				        )
				
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
				        pset = "cmsDrivers/UL20/HHTo2B2G/" + pset_hlt,
				        cmssw_version = cmssw_v_hlt,
				        scram_arch = scram_arch_hlt,
				        condor_submit_params =  condor_submit_params
				        )
				
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
				        pset = "cmsDrivers/UL20/HHTo2B2G/" + pset_reco,
				        cmssw_version = cmssw_v_reco,
				        scram_arch = scram_arch_reco,
				        condor_submit_params =  condor_submit_params
				        )
				
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
				        pset = "cmsDrivers/UL20/HHTo2B2G/" + pset_miniaodsim,
				        cmssw_version = cmssw_v_miniaodsim,
				        scram_arch = scram_arch_miniaodsim,
				        condor_submit_params =  condor_submit_params
				        )

				
				total_summary = {}
				for task in [step1,step2,step3,step4,step5,step6]:
				    task.process()
				    summary = task.get_task_summary()
				    total_summary[task.get_sample().get_datasetname()] = summary
				StatsParser(data=total_summary, webdir="~/public_html/dump/metis_miniAODv2/").do()     

		#time.sleep( 5 * 60 )
		time.sleep(  90 * 60 )


runall("miniAOD_runII_20UL", 200000, 250)
#runall("miniAOD_runII_20UL_test", 5, 5)
