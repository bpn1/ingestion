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
