#!/bin/bash
path="target/scala-2.11/scoverage-report/scoverage.xml"
percentage=`xpath -q -e "string(/scoverage/@statement-rate)" $path`
int_percentage=`echo $percentage | cut -f1 -d"."`
if [ "$int_percentage" -lt 20 ]; then
	color="red"
elif [ "$int_percentage" -lt 40 ]; then
	color="orange"
elif [ "$int_percentage" -lt 60 ]; then
	color="yellow"
elif [ "$int_percentage" -lt 80 ]; then
	color="yellowgreen"
elif [ "$int_percentage" -lt 100 ]; then
	color="green"
elif [ "$int_percentage" -eq 100 ]; then
	color="brightgreen"
else
	echo "Bad percentage $percentage with integer part $int_percentage"
	exit 1
fi
badge_url="https://img.shields.io/badge/coverage-$percentage%-$color.svg"
if [ "$GIT_BRANCH" = "origin/master" ]; then
	filename="master"
else
	filename="latest"
fi
wget -O "/var/lib/jenkins/badges/$filename.svg" $badge_url
echo "Downloaded badge $badge_url"
