from __future__ import print_function
import luigi
import commands
import os

wiki_jar = '/home/hadoop/WikiImport/target/scala-2.10/WikiImport-assembly-1.0.jar'
datalake_jar = '/home/hadoop/DataLakeImport/target/scala-2.10/DataLakeImport-assembly-1.0.jar'

programs = [
    ('WikiDataRDD', wiki_jar),
    ('TagEntities', wiki_jar),
    ('ResolveEntities', wiki_jar),
    ('DataLakeImport', datalake_jar),
    ('FindRelations', datalake_jar)]