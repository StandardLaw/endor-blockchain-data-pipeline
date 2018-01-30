#!/usr/bin/env bash
TEMP_FILE=`mktemp`
curl https://api.coinmarketcap.com/v1/ticker/?limit=0 2>/dev/null | tr -d "\n" > $TEMP_FILE
aws s3 cp --sse AES256 $TEMP_FILE s3://endor-blockchains/ethereum/rates/Inbox/`date -u +%Y-%m-%d-%H-%M.json`
rm -f $TEMP_FILE