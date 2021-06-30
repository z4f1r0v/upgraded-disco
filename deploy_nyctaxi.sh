#!/bin/bash

# NYC Taxi Example
build/sbt package && databricks pipelines deploy nyctaxi.json
