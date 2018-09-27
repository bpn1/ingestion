SPARK_SUBMIT = HADOOP_USER_NAME="ingestion" spark-submit
PYSPARK_SUBMIT = HADOOP_USER_NAME="ingestion" PYSPARK_PYTHON=./SPACY/spacy_env/bin/python spark-submit
HOME = /home/jan.ehmueller
SPARK_SH = $(HOME)/scripts/spark.sh
JAR = $(HOME)/ingestion-assembly-1.0.jar
SPACY_DIR = $(HOME)/pyspark/spacy

###########################
### Sentence Embeddings ###
###########################

sentence-embeddings: sentence-split tokenize embeddings

sentence-split:
	$(SPARK_SH) -m yarn -e 5 -d 1 -c de.hpi.ingestion.sentenceembedding.SentenceSplitter $(JAR)

tokenize:
	cd $(SPACY_DIR); $(PYSPARK_SUBMIT) --num-executors 23 --executor-cores 4 --executor-memory 10G --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 --conf spark.cassandra.connection.host=odin02,odin03,odin04,odin05,odin06,odin07,odin08 --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./SPACY/spacy_env/bin/python --master yarn --archives spacy_env.zip#SPACY pyspark_spacy.py; cd $(HOME)

embeddings:
	$(SPARK_SUBMIT) --class de.hpi.ingestion.framework.JobRunner --num-executors 8 --executor-cores 2 --executor-memory 28G --driver-memory 8G $(JAR) de.hpi.ingestion.sentenceembedding.SentenceEmbeddings

#####################
### Deduplication ###
#####################

implisense: merging-implisense master-update

wikidata: deduplication-wikidata merging-wikidata master-update

dbpedia: deduplication-dbpedia merging-dbpedia master-update

merging-implisense:
	$(SPARK_SH) -m yarn -e 15 -d 16 -c de.hpi.ingestion.datamerge.Merging $(JAR) -c merging_implisense.xml

deduplication-wikidata:
	$(SPARK_SH) -m yarn -e 15 -d 16 -c de.hpi.ingestion.deduplication.Deduplication $(JAR) -c deduplication_wikidata.xml

merging-wikidata:
	$(SPARK_SH) -m yarn -e 15 -d 16 -c de.hpi.ingestion.datamerge.Merging $(JAR) -c merging_wikidata.xml

merging-dbpedia:
	$(SPARK_SH) -m yarn -e 15 -d 16 -c de.hpi.ingestion.datamerge.Merging $(JAR) -c merging_dbpedia.xml

deduplication-dbpedia:
	$(SPARK_SH) -m yarn -e 15 -d 16 -c de.hpi.ingestion.deduplication.Deduplication $(JAR) -c deduplication_dbpedia.xml

master-update:
	$(SPARK_SH) -m yarn -e 8 -d 4 -c de.hpi.ingestion.datamerge.MasterUpdate $(JAR)
	$(SPARK_SH) -m yarn -e 5 -d 2 -c de.hpi.ingestion.datamerge.MasterConnecting $(JAR)
