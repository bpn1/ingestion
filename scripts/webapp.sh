#!/bin/bash
pid="$(ps aux | grep manage.py | grep server | awk '{print $2;}')"
if [ -n "$pid" ]; then
        kill $pid
fi
rm config/cred.json
cp $NEO_CONF config/cred.json
source /home/jan.ehmueller/anaconda2/bin/activate webapp
pip install -r requirements.txt
cd cle
yarn && yarn buildStatic
cd ..
cp ./cle/electron/static/* ./app/public/ && mv ./app/public/index.html ./app/templates/
BUILD_ID=dontKillMe python manage.py server &
