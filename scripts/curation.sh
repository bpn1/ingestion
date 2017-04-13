#!/bin/bash
forever stopall
rm cassandra.config.js
cp $CASSANDRA_CONF cassandra.config.js
npm install
BUILD_ID=dontKillMe forever --minUptime 1 --spinSleepTime 1 start -c "npm run node react" .
