from abstract_pipeline import AbstractPipeline
from task_definition import TaskDefinition
from pipeline_constructor import PipelineConstructor
import luigi

cosine_jar = "/home/bp2016n1/jars/jenkins/ingestion_feature_textmining_34.jar"
extended_links_jar = "/home/bp2016n1/jars/jenkins/ingestion_feature_textmining_197.jar"

# https://docs.google.com/presentation/d/1KhNmbFrYCKe223mJjRMIJJ_CJPJX4Gk8sWdWcUwPlT4/edit?usp=sharing
task_definitions = [TaskDefinition("TextParser", jar=extended_links_jar),
                    TaskDefinition("LinkCleaner", ["TextParser"], jar=extended_links_jar),
                    TaskDefinition("RedirectResolver", ["LinkCleaner"], jar=extended_links_jar),
                    TaskDefinition("LinkAnalysis1", ["RedirectResolver"], "LinkAnalysis", jar=extended_links_jar),
                    TaskDefinition("CompanyLinkFilter", ["LinkAnalysis1"], jar=extended_links_jar),
                    TaskDefinition("LinkAnalysis2", ["CompanyLinkFilter"], "LinkAnalysis", jar=extended_links_jar),
                    TaskDefinition("LocalTrieBuilder", ["CompanyLinkFilter"]),
                    TaskDefinition("AliasTrieSearch", ["LocalTrieBuilder"], jar=extended_links_jar,
                                   command="ssh bp2016n1@sopedu \"HADOOP_USER_NAME='bp2016n1' spark-submit --class de.hpi.ingestion.textmining.AliasTrieSearch --master yarn --num-executors 23 --executor-cores 4 --executor-memory 19G  --conf 'spark.executor.extraJavaOptions=-XX:ThreadStackSize=1000000' " + extended_links_jar + "\""),
                    TaskDefinition("AliasCounter", ["AliasTrieSearch"], jar=extended_links_jar),
                    TaskDefinition("DocumentFrequencyCounter", ["TextParser"], jar=extended_links_jar),
                    TaskDefinition("TermFrequencyCounter", ["CompanyLinkFilter"], jar=extended_links_jar),
                    TaskDefinition("CosineContextComparator",
                                   ["DocumentFrequencyCounter", "AliasCounter", "TermFrequencyCounter",
                                    "LinkAnalysis2"], jar=cosine_jar),
                    TaskDefinition("Classifier", ["CosineContextComparator"])]


class TextminingPipeline(AbstractPipeline):
    name = "Textmining Pipeline"
    force_execution = luigi.BoolParameter(default=False)
    package = "de.hpi.ingestion.textmining"
    task_definitions = task_definitions

    def __init__(self, *args, **kwargs):
        super(TextminingPipeline, self).__init__(*args, **kwargs)
        type(self).define_upstream_task()
