from abstract_pipeline import AbstractPipeline
from task_definition import TaskDefinition
from pipeline_constructor import PipelineConstructor
import luigi

cosine_jar = "/home/bp2016n1/jars/jenkins/ingestion_feature_textmining_34.jar"
extended_links_jar = "/home/bp2016n1/jars/jenkins/ingestion_feature_textmining_197.jar"

# https://docs.google.com/presentation/d/1KhNmbFrYCKe223mJjRMIJJ_CJPJX4Gk8sWdWcUwPlT4/edit?usp=sharing
tasks_definitions = [TaskDefinition("TextParser", jar=extended_links_jar),
                     TaskDefinition("LinkCleaner", ["TextParser"], jar=extended_links_jar),
                     TaskDefinition("RedirectResolver", ["LinkCleaner"], jar=extended_links_jar),
                     TaskDefinition("LinkAnalysis1", ["RedirectResolver"], "LinkAnalysis", jar=extended_links_jar),
                     TaskDefinition("CompanyLinkFilter", ["LinkAnalysis1"], jar=extended_links_jar),
                     TaskDefinition("LinkAnalysis2", ["CompanyLinkFilter"], "LinkAnalysis", jar=extended_links_jar),
                     TaskDefinition("LocalTrieBuilder", ["CompanyLinkFilter"]),
                     TaskDefinition("AliasTrieSearch", ["LocalTrieBuilder"], jar=extended_links_jar),
                     TaskDefinition("AliasCounter", ["AliasTrieSearch"], jar=extended_links_jar),
                     TaskDefinition("DocumentFrequencyCounter", ["TextParser"], jar=extended_links_jar),
                     TaskDefinition("TermFrequencyCounter", ["CompanyLinkFilter"], jar=extended_links_jar),
                     TaskDefinition("CosineContextExtractor",
                                    ["DocumentFrequencyCounter", "AliasCounter", "TermFrequencyCounter",
                                     "LinkAnalysis2"], jar=cosine_jar),
                     TaskDefinition("Classifier", ["CosineContextExtractor"])]


class TextminingPipeline(AbstractPipeline):
    name = "Textmining Pipeline"
    force_execution = luigi.BoolParameter(default=False)
    package_prefix = "de.hpi.ingestion.textmining."
    tasks_dict = PipelineConstructor.define_task_dictionary(tasks_definitions)
    upstream_task = PipelineConstructor(package_prefix).construct_task_dag(tasks_dict, force_execution)
