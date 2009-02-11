#!/bin/sh


pwd=`pwd`
PYTHONPATH=${pwd}:${PYTHONPATH}
export PYTHONPATH

# Command line arguments 
echo $@  echo $#
if [ "$#" < 2  ]; then
    echo "------------------------------------------"
    echo "Usage: run.sh <policy-file-name> <runId> [<kill-jobs>]"
    echo "------------------------------------------"
    exit 0
fi

# --------------------------------------------------------- 
# INPUT PARAMETERS
pipelinePolicyName=${1}
nodelistName=nodelist.scr
runId=${2}

killJobs=true
if [ "$#" -eq 3 ]; then
    killJobs=$3
fi

# --------------------------------------------------------- 
# Extract the number of nodes and the number of nodes & slices from 
# the nodelist file
nodes=`awk -F: '{if($1 !~ /^#/ && $2 ~ /[0-9]+$/ && NF==2) {nodes++}} END {printf("%d", nodes)}' $nodelistName`
usize=`awk -F: '{if($1 !~ /^#/ && $2 ~ /[0-9]+$/ && NF==2) {slices+=$2}} END {printf("%d", slices)}' $nodelistName`


echo "nodes ${nodes}"
echo "nslices $(( $usize - 1))"
echo "usize ${usize}"
echo "killjobs ${killJobs}"

# MPI commands will be in PATH if mpich2 is in build
if [ $killJobs = false ]; then
    echo "Running mpdboot"
    mpdboot --totalnum=${nodes} --file=${nodelistName} --verbose
fi

sleep 3s
echo "Running mpdtrace"
mpdtrace -l
sleep 2s

echo "Running mpiexec"
	
mpiexec -usize ${usize}  -machinefile ${nodelistName} -np 1 runPipeline.py ${pipelinePolicyName} ${runId}

sleep 1s

if [ $killJobs = true ]; then
    echo "Running mpdallexit"
    mpdallexit
else 
    echo "Not running mpdallexit"
fi
