import sys, os
import time
import itertools
import json

from metis.Sample import DBSSample, DirectorySample, Sample
from metis.CondorTask import CondorTask
from metis.StatsParser import StatsParser

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--tag", help = "tag to identify this set of babies", type=str)
parser.add_argument("--filter", help = "only process mc/data with some requirement (e.g. 2016MC, 2017Data)", default="", type=str)
parser.add_argument("--dsfilter", help = "only process mc/data with some name pattern(e.g. DY***)", default="", type=str)
parser.add_argument("--soft_rerun", help = "don't remake tarball", action="store_true")
parser.add_argument("--skip_local", help = "don't submit jobs for local samples", action = "store_true")
parser.add_argument("--skip_central", help = "don't submit jobs for central samples", action = "store_true")
args = parser.parse_args()

from dsdefs_centralminiaod_UL import dsdefs_NMSSM_Ygg_Hbb
# for local inputs
local_sets = []

# some job configurations
job_dir = "Summer20UL_nanoAODv9/"
job_tag = args.tag
job_filter = args.filter
ds_filter = args.dsfilter
skip_central = args.skip_central
ceph_path = "{0}".format(job_dir)

cmssw_ver = "CMSSW_10_6_26"

DOSKIM = False 

exec_path = "condor_exe_ceph.sh"

if not args.soft_rerun:
    os.system("rm no_fixedGridRho_CMSSW/package.tar.gz")
    os.system(" cd no_fixedGridRho_CMSSW; XZ_OPT='-3e -T24' tar -Jc --exclude='.git' --exclude='*.root' --exclude='*.tar*' --exclude='*.out' --exclude='*.err' --exclude='*.log' --exclude '*.nfs*' -f package.tar.gz %s; cd .." % cmssw_ver)
    #os.system("rm package.tar.gz")
    #os.system("XZ_OPT='-3e -T24' tar -Jc --exclude='.git' --exclude='*.root' --exclude='*.tar*' --exclude='*.out' --exclude='*.err' --exclude='*.log' --exclude '*.nfs*' -f package.tar.gz %s" % cmssw_ver)

total_summary = {}
while True:
    allcomplete = True

    # Loop through central samples
    for ds,fpo,args in dsdefs_NMSSM_Ygg_Hbb_ext1[:]:
        if skip_central: continue
        if (job_filter != "") and (args not in job_filter) : continue         
        if (ds_filter != "") and (ds_filter not in ds) : continue         
        sample = DBSSample( dataset=ds )
        print(ds, args)

        task = CondorTask(
                sample = sample,
                open_dataset = False,
                files_per_output = fpo,
                output_name = "nanoaod.root",
                tag = job_tag,
								cmssw_version = cmssw_ver,
                executable = exec_path,
                tarfile = "no_fixedGridRho_CMSSW/package.tar.gz",
                condor_submit_params = {"sites": "T2_US_UCSD,T2_US_CALTECH,T2_US_WISCONSIN,T2_US_Vanderbilt,T2_US_Florida", # other_sites can be good_sites, your own list, etc.
                    "classads": [["SingularityImage","/cvmfs/singularity.opensciencegrid.org/cmssw/cms:rhel7-m202006"]],
										"use_xrootd":True
								},
                special_dir = ceph_path,
                arguments = args.replace(" ","|")
                )
        task.process()
        allcomplete = allcomplete and task.complete()
        # save some information for the dashboard
        total_summary[ds] = task.get_task_summary()
        with open("summary.json", "w") as f_out:
            json.dump(total_summary, f_out, indent=4, sort_keys=True)



    # parse the total summary and write out the dashboard
    StatsParser(data=total_summary, webdir="~/public_html/dump/metis_NMSSM_v9/").do()
    os.system("chmod -R 755 ~/public_html/dump/metis_NMSSM_v9")
    if allcomplete:
        print ""
        print "Job={} finished".format(job_tag)
        print ""
        break
    sleep_time	= 1.5 * 60 * 60
    print "Sleeping " + str(sleep_time / 60) + " minutes ..."
    time.sleep(sleep_time)
