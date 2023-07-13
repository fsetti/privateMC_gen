Make private MC or nanoAOD based on ProjectMetis

https://github.com/aminnj/ProjectMetis

Follow instructions in ProjectMetis to setup the production via Condor Grid submission.

### Gridpack to miniAODv2
To produce miniAOD from gridpack, refer to scripts such as `signal_20UL_bbgg_chainpset.py`. This basically loads the psets and configurations (e.g. CMSSW version) for each step of the production and sets up the condor job and bookkeeping.

###  miniAODv2 to nanoAOD
To run this step we had to modify the CMSSW environment and compress it into a tarball. Please use the file in `no_fixedGridRho_CMSSW/package.tar.gz` to reproduce the nanoAOD formate used in HIG-22-012. The script to use for the conversion is `mini2nanoAOD_local.py` (for locally available miniAOD files) or `mini2nanoAOD_resonant.py` (for centrally available miniAOD files)
