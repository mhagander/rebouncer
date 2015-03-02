#!/bin/bash

# Simlpe nagios plugin to query the rebouncer webserver nagios status

URL=$1

DATA=$(curl -4 -f -s ${URL})
if [ "$?" != "0" ]; then
   echo "CRITICAL: Failed to talk to rebouncer!"
   exit 2
fi

if [[ $DATA == WARNING* ]]; then
   echo $DATA
   exit 1
fi

if [[ $DATA == CRITICAL* ]]; then
   echo $DATA
   exit 2
fi

echo $DATA
exit 0

