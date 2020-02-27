#!/bin/bash

cat sources.txt | xargs -I% curl -SsfLO %

tar czvf ./data.tar.gz *.dat


