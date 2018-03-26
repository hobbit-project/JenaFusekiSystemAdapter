#!/bin/bash

FOLDER=$1
GRAPH_URI=$2
DATASET_PATH=/jenafuseki/data

# use tdbloader2 for the initial dataset and then use tdbloader
if [ $GRAPH_URI = "http://graph.version.0" ]; then
  /jena/bin/tdbloader2 --loc $DATASET_PATH $FOLDER/*
else
  /jena/bin/tdbloader --loc $DATASET_PATH --graph=$GRAPH_URI $FOLDER/*
fi
