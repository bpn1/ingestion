#!/bin/bash
forever stopall
rm cassandraAuth.config.js
cp $CASSANDRA_CONF cassandraAuth.config.js
npm install
BUILD_ID=dontKillMe forever --minUptime 1 --spinSleepTime 1 start -c "npm start" .