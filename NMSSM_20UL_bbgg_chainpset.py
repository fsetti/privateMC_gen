import os
from metis.CMSSWTask import CMSSWTask
from metis.Sample import DirectorySample, DummySample
from metis.Path import Path
from metis.StatsParser import StatsParser
import time

import sys
sys.path.append("/home/users/fsetti/public_html/privateMC_gen")
from update_pset_NMSSM import edit_pset_NMSSM_Ygg_Hbb, edit_pset_NMSSM_Ybb_Hgg
from allconfig import NMSSM_20UL_bbgg 

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


mxs	= [ '600', '650', '700' ]
mys	= [ '90', '95', '96', '97', '98', '99', '100' ]

def runall(special_dir, total_nevents, events_per_output):

	proc_tag = "fixBug17"

	for _ in range(2500):

		for mx in mxs:
			for my in mys:

				for year in years:

					proc	= "NMSSM_XYH_Y_gg_H_bb"
					coupling = 'MX_%s_MY_%s'%(mx,my)
					tag	=	'%s_%s_%s'%(proc,coupling,year) 
		
					total_nevents_tmp = total_nevents				
					steps = []
		
					if '2016' in year:
						total_nevents_tmp /= 2
		
					try:
						cmssw_v_gen = NMSSM_20UL_bbgg[year]["cmssw_v_gen"] 
						pset_gen = NMSSM_20UL_bbgg[year]["pset_gen"]
						pset_gen = pset_gen.replace('_1_cfg.py','_1_cfg_%s.py'%(coupling))
						scram_arch_gen = NMSSM_20UL_bbgg[year]["scram_arch_gen"]

						if 'Y_bb_H_gg' in proc:
							edit_pset_NMSSM_Ybb_Hgg( coupling, 'MX_600_MY_90' , year  )					
						elif 'Y_gg_H_bb' in proc:
							edit_pset_NMSSM_Ygg_Hbb( coupling, 'MX_600_MY_90' , year  )
						else:
							print("Process not recognized!")
							sys.exit()

						step1 = CMSSWTask(
						        # Change dataset to something more meaningful (but keep STEP1, as we use this 
						        # for string replacement later); keep N=1
						        sample = DummySample(N=1, dataset="/"+ tag+ "_STEP1"),
						        # A unique identifier
						        tag = proc_tag,
						        special_dir = special_dir,
						        events_per_output = events_per_output,
						        total_nevents = total_nevents_tmp,
						        # We have one input dummy file, so this must be True
						        split_within_files = True,
						        pset = "cmsDrivers/UL20/"+proc+"/" + pset_gen,
		            		additional_input_files = [ "/ceph/cms/store/user/fsetti/private_gridpacks/%s_%s_slc7_amd64_gcc700_CMSSW_10_6_19_tarball.tar.xz"%(proc,coupling) ],
						        cmssw_version = cmssw_v_gen,
						        scram_arch = scram_arch_gen,
						        condor_submit_params =  condor_submit_params
						       )
						steps += [ step1 ]
		
					except:
						print("Could not process STEP1 for: " + proc + " for " + year )
		
		
					try:
						cmssw_v_sim = NMSSM_20UL_bbgg[year]["cmssw_v_sim"] 
						pset_sim = NMSSM_20UL_bbgg[year]["pset_sim"]
						scram_arch_sim = NMSSM_20UL_bbgg[year]["scram_arch_sim"]
						
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
						cmssw_v_mix = NMSSM_20UL_bbgg[year]["cmssw_v_mix"] 
						pset_mix = NMSSM_20UL_bbgg[year]["pset_mix"]
						scram_arch_mix = NMSSM_20UL_bbgg[year]["scram_arch_mix"]
						
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
						cmssw_v_hlt = NMSSM_20UL_bbgg[year]["cmssw_v_hlt"] 
						pset_hlt = NMSSM_20UL_bbgg[year]["pset_hlt"]
						scram_arch_hlt = NMSSM_20UL_bbgg[year]["scram_arch_hlt"]
						
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
						cmssw_v_reco = NMSSM_20UL_bbgg[year]["cmssw_v_reco"] 
						pset_reco = NMSSM_20UL_bbgg[year]["pset_reco"]
						scram_arch_reco = NMSSM_20UL_bbgg[year]["scram_arch_reco"]
						
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
						cmssw_v_miniaodsim = NMSSM_20UL_bbgg[year]["cmssw_v_miniaodsim"] 
						pset_miniaodsim = NMSSM_20UL_bbgg[year]["pset_miniaodsim"]
						scram_arch_miniaodsim = NMSSM_20UL_bbgg[year]["scram_arch_miniaodsim"]
		
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
		#time.sleep(  60 * 60 )


runall("miniAOD_runII_20UL", 200000, 250)
#runall("miniAOD_runII_20UL_test", 10000, 250)
