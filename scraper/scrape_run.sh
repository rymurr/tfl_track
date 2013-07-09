#!/bin/bash

DIR=~/tfl_track
source $DIR/virtenv/bin/activate
python $DIR/scraper/prediction_summary.py
sleep 30
python $DIR/scraper/prediction_summary.py
