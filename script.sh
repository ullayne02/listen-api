#!/bin/bash

# activate venv
source /Users/ullayne/listen-api/venv/bin/activate

# run the script
python /Users/ullayne/listen-api/ingest/data_collect.py

# deactivate
deactivate