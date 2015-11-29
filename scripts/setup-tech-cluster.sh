#!/bin/bash

# Created on Nov 25, 2015
# Author : Gagan M Gowda (theTechie)

echo "Fetching Cluster IPs from AWS..."

# Get IPs of all instances using AWS CLI
#  - filter instances based on ami-id (to identify a zht+ cluster)
#  - filter instances using instance-state-code (for Running instances)
#  - copy zht+ binary and configs using pSSH
#  - start zht+ server on all cluster nodes using pSSH

AMI_ID="ami-a0afbcc1"		# AMI used for cluster node
INSTANCE_STATE_CODE="16"	# 0 (pending), 16 (running), 32 (shutting-down), 48 (terminated), 64 (stopping), 80 (stopped)
PRIVATE_KEY="$HOME/CS550.pem"		# ssh private-key file
HOST_FILE_LOCATION="confs/*"	# location of the files to be copied to remote node
REMOTE_FILE_LOCATION="$HOME/tech-eval"	# target location on remote node
SERVER_PORT=50000		# port on which Server will run
TAG="cs550-cluster-node" # tag assigned to cluster nodes

GET_IP="aws ec2 describe-instances --query "Reservations[*].Instances[*].PrivateIpAddress" --filter Name=instance-state-code,Values=$INSTANCE_STATE_CODE Name=image-id,Values=$AMI_ID Name=tag-value,Values=$TAG --output=text"

IP_LIST=$($GET_IP)

(for i in $IP_LIST; do
	echo $i $SERVER_PORT
done) | tee confs/peers.conf

echo "peers.conf generated !"

# TODO : Use the parallel option -p  for faster copying. How to arrive at the parallelization number ?
# Copy ZHT binary and configs to remote node $HOME
for i in $IP_LIST; do
	echo "connect to $i and copy configs"
	parallel-scp -H $i -x "-oStrictHostKeyChecking=no -i $PRIVATE_KEY" $HOST_FILE_LOCATION $REMOTE_FILE_LOCATION
done

# Start ZHT server at remote node
for i in $IP_LIST; do
	echo "connect to $i and start dht server"
        parallel-ssh -H $i -x "-oStrictHostKeyChecking=no -i $PRIVATE_KEY" "nohup ./tech-eval/start-server $SERVER_PORT > server-log.out 2> server-log.err < /dev/null &"
done