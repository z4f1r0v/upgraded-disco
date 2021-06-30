#!/bin/bash

# Wiki Example
build/sbt package && databricks pipelines deploy wiki.json
