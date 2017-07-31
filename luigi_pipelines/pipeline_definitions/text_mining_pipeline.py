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

wikipedia_package = "de.hpi.ingestion.dataimport.wikipedia"
textmining_package = "de.hpi.ingestion.textmining"
preprocessing_package = textmining_package + ".preprocessing"
nel_package = textmining_package + ".nel"
re_package = textmining_package + ".re"
hdfs_jar = "/home/bp2016n1/jars/jenkins/ingestion_323_textmining_feature.jar"

# https://github.com/bpn1/ingestion/wiki/Text-Mining
task_definitions = [
    # Preprocessing
    TaskDefinition("WikipediaImport", package="wikipedia_package"),
    TaskDefinition("TextParser", ["WikipediaImport"], package=preprocessing_package),
    TaskDefinition("DocumentFrequencyCounter", ["TextParser"], package=preprocessing_package),
    TaskDefinition("LinkCleaner", ["TextParser"], package=preprocessing_package),
    TaskDefinition("RedirectResolver", ["LinkCleaner"], package=preprocessing_package),
    TaskDefinition("LinkAnalysis1", ["RedirectResolver"], "LinkAnalysis", package=preprocessing_package),
    TaskDefinition("CompanyLinkFilter", ["LinkAnalysis1"], package=preprocessing_package),
    TaskDefinition("ReducedLinkAnalysis1", ["CompanyLinkFilter"], "ReducedLinkAnalysis", package=preprocessing_package),
    # TaskDefinition("LocalTrieBuilder", ["CompanyLinkFilter"], package=preprocessing_package),
    TaskDefinition("AliasTrieSearch", ["CompanyLinkFilter"], package=preprocessing_package, jar=hdfs_jar,
                   command="ssh bp2016n1@sopedu \"HADOOP_USER_NAME='bp2016n1' spark-submit --class de.hpi.ingestion.textmining.AliasTrieSearch --master yarn --num-executors 23 --executor-cores 4 --executor-memory 19G  --conf 'spark.executor.extraJavaOptions=-XX:ThreadStackSize=1000000' " + hdfs_jar + "\""),
    TaskDefinition("LinkExtender", ["ReducedLinkAnalysis", "AliasTrieSearch"], package=preprocessing_package,
                   command="ssh bp2016n1@sopedu \"HADOOP_USER_NAME='bp2016n1' spark-submit --class de.hpi.ingestion.textmining.LinkExtender --master yarn --driver-memory 16G --driver-java-options -Xss1g --num-executors 23 --executor-cores 4 --executor-memory 15G --conf 'spark.executor.extraJavaOptions=-XX:ThreadStackSize=1000000' " + TaskDefinition.ingestion_jar + "\""),
    TaskDefinition("TermFrequencyCounter", ["LinkExtender"], package=preprocessing_package),
    TaskDefinition("LinkAnalysis2", ["LinkExtender"], "LinkAnalysis", package=preprocessing_package),
    TaskDefinition("ReducedLinkAnalysis2", ["LinkAnalysis2"], "ReducedLinkAnalysis", package=preprocessing_package),
    TaskDefinition("AliasCounter", ["ReducedLinkAnalysis2"], package=preprocessing_package),
    TaskDefinition("CosineContextComparator", ["DocumentFrequencyCounter", "AliasCounter",
                                               "TermFrequencyCounter"], package=preprocessing_package),

    # Classifier Training
    TaskDefinition("ClassifierTraining", ["CosineContextComparator"], package=textmining_package),

    # Named Entity Linking
    TaskDefinition("WikipediaReduction", ["TextParser"], package=nel_package),
    TaskDefinition("SpiegelImport", package=nel_package),
    TaskDefinition("ArticleTrieSearch", ["WikipediaReduction", "SpiegelImport"], package=nel_package),
    TaskDefinition("TextNEL", ["ClassifierTraining"], package=nel_package),
    TaskDefinition("HtmlGenerator", ["TextNEL"], package=nel_package),

    # Relation Extraction
    TaskDefinition("RelationSentenceParser", ["TextNEL"], package=re_package),
    TaskDefinition("CooccurrenceCounter", ["RelationSentenceParser"], package=re_package),
    TaskDefinition("RelationClassifier", ["CooccurrenceCounter"], package=re_package)
]


class TextMiningPipeline(AbstractPipeline):
    name = "Text Mining Pipeline"
    package = textmining_package
    task_definitions = task_definitions
