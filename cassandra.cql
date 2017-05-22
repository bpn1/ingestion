-- Wikidumps
CREATE KEYSPACE wikidumps WITH REPLICATION = {
    'class' : 'SimpleStrategy',
    'replication_factor' : 3
};

-- Wikidumps.Wikipedia
CREATE TYPE wikidumps.link (
    alias text,
    page text,
    offset int,
    context map<text, int>
);

CREATE TYPE wikidumps.entitylink (
    alias text,
    entity text,
    offset int
);

CREATE TABLE wikidumps.parsedwikipedia (
    title text PRIMARY KEY,
    categorylinks frozen<list<frozen<link>>>,
    context map<text, int>,
    disambiguationlinks frozen<list<frozen<link>>>,
    foundaliases list<text>,
    linkswithcontext frozen<list<frozen<link>>>,
    listlinks list<frozen<link>>,
    rawextendedlinks frozen<list<frozen<extendedlink>>>,
    templatelinks list<frozen<link>>,
    text text,
    textlinks list<frozen<link>>,
    triealiases frozen<list<frozen<triealias>>>
);

CREATE TABLE wikidumps.wikipedia (
    title text PRIMARY KEY,
    text text
);

CREATE TABLE wikidumps.wikipedialinks (
    alias text PRIMARY KEY,
    pages map<text, int>,
    linkoccurrences int,
    totaloccurrences int
);

CREATE TABLE wikidumps.wikipediapages (
    page text PRIMARY KEY,
    aliases map<text, int>
);

CREATE TABLE wikidumps.wikipediadocfreq (
    word text PRIMARY KEY,
    count int
);

CREATE TABLE wikidumps.wikipediatfidf (
    page text PRIMARY KEY,
    tokens map<text, double>
);

CREATE TABLE wikidumps.wikipediasentences (
   articletitle text,
   articleoffset int,
   text text,
   entities frozen<list<frozen<entitylink>>>,
   PRIMARY KEY (articletitle, articleoffset)
);

CREATE TABLE wikidumps.featureentries (
    id uuid PRIMARY KEY,
    alias text,
    correct boolean,
    cosine_sim double,
    entity text,
    prob_entity double,
    prob_link double
);

CREATE TYPE wikidumps.precisionrecalldatatuple (
    threshold double,
    precision double,
    recall double,
    fscore double
);

CREATE TABLE wikidumps.sim_measure_stats (
    id timeuuid PRIMARY KEY,
    comment text,
    data list<frozen<precisionrecalldatatuple>>
);

CREATE TYPE wikidumps.triealias (
	alias text,
	offset int,
	context map<text, int>
);

CREATE TABLE wikidumps.spiegel (
    id text PRIMARY KEY,
    title text,
    text text,
    url text
);

CREATE TABLE wikidumps.redirects (
	alias text PRIMARY KEY,
	page text
);

-- Wikidumps.Wikidata
CREATE TABLE wikidumps.wikidata (
    id text PRIMARY KEY,
    aliases list<text>,
    data map<text, frozen<list<text>>>,
    description text,
    entitytype text,
    enwikiname text,
    instancetype text,
    label text,
    wikiname text
);

-- Wikidumps.DBpedia
CREATE TABLE wikidumps.dbpedia (
    dbpedianame text PRIMARY KEY,
    data map<text, frozen<list<text>>>,
    description text,
    instancetype text,
    label text,
    wikipageid text,
    wikidataid text
);

CREATE TYPE wikidumps.extendedlink (
    alias text,
    page map<text, int>,
    offset int
);

-- Datalake
CREATE KEYSPACE datalake WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

CREATE TYPE datalake.version (
    version timeuuid,
    value frozen<list<text>>,
    validity frozen<map<text, text>>,
    datasources frozen<list<text>>,
    timestamp timestamp,
    program text
);

CREATE TYPE datalake.subject (
    id uuid,
    aliases list<text>,
    aliases_history list<frozen<version>>,
    category text,
    category_history list<frozen<version>>,
    name text,
    name_history list<frozen<version>>,
    properties map<text, frozen<list<text>>>,
    properties_history map<text, frozen<list<frozen<version>>>>,
    relations map<uuid, frozen<map<text, text>>>,
    relations_history map<uuid, frozen<map<text, frozen<list<frozen<version>>>>>>
);

CREATE TABLE datalake.subject (
    id uuid PRIMARY KEY,
    aliases list<text>,
    aliases_history list<frozen<version>>,
    category text,
    category_history list<frozen<version>>,
    name text,
    name_history list<frozen<version>>,
    properties map<text, frozen<list<text>>>,
    properties_history map<text, frozen<list<frozen<version>>>>,
    relations map<uuid, frozen<map<text, text>>>,
    relations_history map<uuid, frozen<map<text, frozen<list<frozen<version>>>>>>
);

CREATE TABLE datalake.subject_dbpedia (
    id uuid PRIMARY KEY,
    aliases list<text>,
    aliases_history list<frozen<version>>,
    category text,
    category_history list<frozen<version>>,
    name text,
    name_history list<frozen<version>>,
    properties map<text, frozen<list<text>>>,
    properties_history map<text, frozen<list<frozen<version>>>>,
    relations map<uuid, frozen<map<text, text>>>,
    relations_history map<uuid, frozen<map<text, frozen<list<frozen<version>>>>>>
);

CREATE TABLE datalake.subject_wikidata (
    id uuid PRIMARY KEY,
    aliases list<text>,
    aliases_history list<frozen<version>>,
    category text,
    category_history list<frozen<version>>,
    name text,
    name_history list<frozen<version>>,
    properties map<text, frozen<list<text>>>,
    properties_history map<text, frozen<list<frozen<version>>>>,
    relations map<uuid, frozen<map<text, text>>>,
    relations_history map<uuid, frozen<map<text, frozen<list<frozen<version>>>>>>
);

CREATE TABLE datalake.version (
    version timeuuid PRIMARY KEY,
    datasources list<text>,
    program text,
    timestamp timestamp
);

CREATE TABLE datalake.object (
    id uuid PRIMARY KEY,
    aliases list<text>,
    aliases_history list<frozen<version>>,
    category text,
    category_history list<frozen<version>>,
    name text,
    name_history list<frozen<version>>,
    properties map<text, frozen<list<text>>>,
    properties_history map<text, frozen<list<frozen<version>>>>
);

CREATE TYPE datalake.blockstats (
    key text,
    numsubjects int,
    numstaging int,
    precision double
);

CREATE TABLE datalake.blockingstats (
    jobid timeuuid,
    schemetag text,
    comment text,
    data frozen<set<frozen<blockstats>>>,
    PRIMARY KEY (jobid, schemetag)
);

CREATE TABLE datalake.duplicatecandidates (
    subject_id uuid PRIMARY KEY,
    candidates list<frozen<tuple<subject, text, double>>>
);

CREATE TABLE datalake.sim_measure_stats (
    id timeuuid PRIMARY KEY,
    comment text,
    data text
);

-- Evaluation
CREATE KEYSPACE evaluation WITH REPLICATION = {
    'class' : 'SimpleStrategy',
    'replication_factor' : 3
};

-- Types
CREATE TYPE evaluation.version (
    version timeuuid,
    value frozen<list<text>>,
    validity frozen<map<text, text>>,
    datasources frozen<list<text>>,
    timestamp timestamp,
    program text
);

CREATE TYPE evaluation.precisionrecalldatatuple (
    threshold double,
    precision double,
    recall double,
    fscore double
);

CREATE TYPE evaluation.subject (
    id uuid,
    aliases list<text>,
    aliases_history list<frozen<version>>,
    category text,
    category_history list<frozen<version>>,
    name text,
    name_history list<frozen<version>>,
    properties map<text, frozen<list<text>>>,
    properties_history map<text, frozen<list<frozen<version>>>>,
    relations map<uuid, frozen<map<text, text>>>,
    relations_history map<uuid, frozen<map<text, frozen<list<frozen<version>>>>>>
);

-- Tables
CREATE TABLE evaluation.goldstandard (
    id1 uuid PRIMARY KEY,
    id2 uuid
);

CREATE TABLE evaluation.subject_dbpedia (
    id uuid PRIMARY KEY,
    aliases list<text>,
    aliases_history list<frozen<version>>,
    category text,
    category_history list<frozen<version>>,
    name text,
    name_history list<frozen<version>>,
    properties map<text, frozen<list<text>>>,
    properties_history map<text, frozen<list<frozen<version>>>>,
    relations map<uuid, frozen<map<text, text>>>,
    relations_history map<uuid, frozen<map<text, frozen<list<frozen<version>>>>>>
);

CREATE TABLE evaluation.subject_wikidata (
    id uuid PRIMARY KEY,
    aliases list<text>,
    aliases_history list<frozen<version>>,
    category text,
    category_history list<frozen<version>>,
    name text,
    name_history list<frozen<version>>,
    properties map<text, frozen<list<text>>>,
    properties_history map<text, frozen<list<frozen<version>>>>,
    relations map<uuid, frozen<map<text, text>>>,
    relations_history map<uuid, frozen<map<text, frozen<list<frozen<version>>>>>>
);

CREATE TABLE evaluation.sim_measure_stats (
    id uuid PRIMARY KEY,
    data list<frozen<precisionrecalldatatuple>>,
    comment text
);

CREATE TABLE evaluation.version (
    version timeuuid PRIMARY KEY,
    datasources list<text>,
    program text,
    timestamp timestamp
);

CREATE TABLE evaluation.dbpedia_wikidata_deduplication (
    subject_id uuid PRIMARY KEY,
    candidates list<frozen<tuple<subject, text, double>>>
);

CREATE TABLE evaluation.dbpedia_wikidata_deduplication_only_uuid (
    subject_id uuid PRIMARY KEY,
    candidates list<frozen<tuple<uuid, text, double>>>
);

CREATE TABLE evaluation.featureentries (
	id uuid PRIMARY KEY,
	subject frozen<subject>,
	staging frozen<subject>,
	score map<text, frozen<list<double>>>,
	correct boolean
);
