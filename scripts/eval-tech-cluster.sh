#!/bin/bash

# Created on Nov 25, 2015
# Author : Gagan M Gowda (theTechie)

echo "Fetching Cluster IPs from AWS..."

# Get IPs of all instances using AWS CLI
#  - filter instances based on ami-id (to identify a zht+ cluster)
#  - filter instances using instance-state-code (for Running instances)
#  - copy zht+ binary and configs using pSSH
#  - start zht+ server on all cluster nodes using pSSH

AMI_ID="ami-a17765c0"		# AMI used for cluster node
INSTANCE_STATE_CODE="16"	# 0 (pending), 16 (running), 32 (shutting-down), 48 (terminated), 64 (stopping), 80 (stopped)
PRIVATE_KEY="$HOME/CS550.pem"		# ssh private-key file
HOST_FILE_LOCATION="confs/*"	# location of the files to be copied to remote node
REMOTE_FILE_LOCATION="$HOME/tech-eval"	# target location on remote node
SERVER_PORT=50000		# port on which ZHT Server will run
TAG="cs550-cluster-node" # tag assigned to cluster nodes

GET_IP="aws ec2 describe-instances --query "Reservations[*].Instances[*].PrivateIpAddress" --filter Name=instance-state-code,Values=$INSTANCE_STATE_CODE Name=image-id,Values=$AMI_ID Name=tag-value,Values=$TAG --output=text"

IP_LIST=$($GET_IP)

#<script> <scale> <iterations>

# Start Evaluation; output output in console and also store in  'output' folder with hostnames
for (( i=0; i < $1; i++ ))
do
	RANGE=$(($(($i+1))*$2));
	echo "connect to ${IP_LIST[$i]} and start tech DB (dht) evaluation"
        parallel-ssh -H ${IP_LIST[$i]} -x "-oStrictHostKeyChecking=no -i $PRIVATE_KEY" -i -o output -e error "node tech-eval/eval/test_client -c tech-eval/peers.conf -k $RANGE -i $2"
done