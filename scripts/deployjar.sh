#!/bin/sh
branch_prefix="origin/"
# Remove prefix of branch name
local_bname=${GIT_BRANCH#$branch_prefix}
# Replace / with _
cleaned_bname=$(echo $local_bname | sed "s#/#_#g")
jar_name="ingestion_$cleaned_bname.jar"
scp target/scala-2.11/ingestion-assembly-1.0.jar bp2016n1@sopedu:~/jars/jenkins/$jar_name
echo "Deployed $jar_name to sopedu."
timestamp="`date +"%H:%M:%S"`"
log_message="$timestamp\t$GIT_COMMIT\t$jar_name"
echo $log_message | ssh bp2016n1@sopedu "cat >> ~/jars/jenkins/jenkins.log"
