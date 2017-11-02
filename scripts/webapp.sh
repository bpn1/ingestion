#!/bin/bash
pid="$(ps aux | grep manage.py | grep server | awk '{print $2;}')"
if [ -n "$pid" ]; then
    kill $pid
fi
rm config/cred.json
cp $NEO_CONF config/cred.json
virtualenv flask
cd flask
source bin/activate
pip install -r requirements.txt
pip install flask
pip install Flask-Bootstrap
pip install Flask-Login
pip install Flask-sqlalchemy
pip install flask-restful
pip install py2neo==2.0.9
pip install elasticsearch
pip install Flask-Script
pip install Flask-Migrate
pip install Flask-Wtf
cd ..
BUILD_ID=dontKillMe python manage.py server &
deactivate
