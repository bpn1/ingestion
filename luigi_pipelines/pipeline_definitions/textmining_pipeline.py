"""
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from pipeline_definitions.abstract_pipeline import AbstractPipeline
from models.task_definition import TaskDefinition

hdfs_jar = "/home/bp2016n1/jars/jenkins/ingestion_323_textmining_feature.jar"

# https://docs.google.com/presentation/d/1KhNmbFrYCKe223mJjRMIJJ_CJPJX4Gk8sWdWcUwPlT4/edit?usp=sharing
task_definitions = [TaskDefinition("TextParser"),
                    TaskDefinition("LinkCleaner", ["TextParser"],),
                    TaskDefinition("RedirectResolver", ["LinkCleaner"]),
                    TaskDefinition("LinkAnalysis1", ["RedirectResolver"], "LinkAnalysis"),
                    TaskDefinition("CompanyLinkFilter", ["LinkAnalysis1"]),
                    TaskDefinition("LinkAnalysis2", ["CompanyLinkFilter"], "LinkAnalysis"),
                    TaskDefinition("LinkExtender", ["LinkAnalysis2"],
                                   command="ssh bp2016n1@sopedu \"HADOOP_USER_NAME='bp2016n1' spark-submit --class de.hpi.ingestion.textmining.LinkExtender --master yarn --driver-memory 16G --driver-java-options -Xss1g --num-executors 23 --executor-cores 4 --executor-memory 15G --conf 'spark.executor.extraJavaOptions=-XX:ThreadStackSize=1000000' " + TaskDefinition.ingestion_jar + "\""),
                    # TaskDefinition("LocalTrieBuilder", ["CompanyLinkFilter"]),
                    TaskDefinition("AliasTrieSearch", ["CompanyLinkFilter"], jar=hdfs_jar,
                                   command="ssh bp2016n1@sopedu \"HADOOP_USER_NAME='bp2016n1' spark-submit --class de.hpi.ingestion.textmining.AliasTrieSearch --master yarn --num-executors 23 --executor-cores 4 --executor-memory 19G  --conf 'spark.executor.extraJavaOptions=-XX:ThreadStackSize=1000000' " + hdfs_jar + "\""),
                    TaskDefinition("AliasCounter", ["AliasTrieSearch"]),
                    TaskDefinition("DocumentFrequencyCounter", ["TextParser"]),
                    TaskDefinition("TermFrequencyCounter", ["CompanyLinkFilter"]),
                    TaskDefinition("CosineContextComparator", ["DocumentFrequencyCounter", "AliasCounter",
                                                               "TermFrequencyCounter", "LinkAnalysis2"]),
                    TaskDefinition("ClassifierTraining", ["CosineContextComparator"])]


class TextminingPipeline(AbstractPipeline):
    name = "Textmining Pipeline"
    package = "de.hpi.ingestion.textmining"
    task_definitions = task_definitions
