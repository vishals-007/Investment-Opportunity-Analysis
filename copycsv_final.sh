#!/bin/bash

cd /home/talentum/csvfile_final
mkdir -p /home/talentum/shared/grafana/final/$(date +"%m_%d_%Y")
mv /home/talentum/shared/grafana/final/portfolio_details.csv /home/talentum/shared/grafana/final/$(date +"%m_%d_%Y")/
cp part* /home/talentum/shared/grafana/final/portfolio_details.csv
