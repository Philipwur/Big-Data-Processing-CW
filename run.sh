#!/bin/bash
python $1 -r hadoop --output-dir $3 --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/$2
