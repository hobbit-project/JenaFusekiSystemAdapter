#!/bin/bash

FOLDER=$1
GRAPH_URI=$2
DATASET_PATH=/jenafuseki/data

/jena-fuseki/tdbloader --loc $DATASET_PATH --graph=$GRAPH_URI $FOLDER/*

