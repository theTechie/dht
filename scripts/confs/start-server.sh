#!/bin/bash

echo "Starting Tech DB (DHT) Server.."

cd tech-eval

node client.js -c peers.conf -p $1

echo "Tech DB (DHT) Server Started !"