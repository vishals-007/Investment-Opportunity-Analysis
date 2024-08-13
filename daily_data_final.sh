#!/bin/bash

mkdir -p /home/talentum/shared/Project/Project_codes/source/$(date +"%m_%d_%Y")
cd /home/talentum/shared/Project/Project_codes/source
cp *.json /home/talentum/shared/Project/Project_codes/source/$(date +"%m_%d_%Y") 
rm -r *.json

stockList="AAPL ABBV ABT ADBE ADP AMGN AMT AMZN AROW ARTNA AXP AZN BA BABA BAC BCAL BHP BKNG BTI CAT CECO CHEF CL CMRX COCO CRM CRML CSCO CTOS CVX DEO DIS ELLO EQIX ETN FAT GCMG GE GOOGL GRC GS GWRS HIMX HIPO HON IBM ICHR ISRG JNJ JPM KLG KMB KO LIN LLY LMT MA MCD MDLZ META MS MSFT MXL NFLX NKE NSPR NTES NVDA ORCL PCYO PDD PFE PG PLCE PM PNNT PNST RIO SAP SIRI SNY SPOT SRG SUP TCI TH TJX TM TSLA TSM UAN UL UNH UNP V VZ WFC WMG WMT YTRA"

for stock in $stockList;
do
	wget -O $stock.json https://financialmodelingprep.com/api/v3/historical-price-full/$stock?apikey=WAitrN1PEcPHKKuucTFtgfnpDD1d2KIu
done

