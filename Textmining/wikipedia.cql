CREATE TYPE wikidumps.link (
    alias text,
    page text,
    offset int
);

CREATE TABLE wikidumps.parsedwikipedia (
    title text PRIMARY KEY,
    links frozen<list<frozen<link>>>,
    text text
);

CREATE TABLE wikidumps.wikipedia (
    title text PRIMARY KEY,
    text text
);

CREATE TABLE wikidumps.wikipediaaliases (
    alias text PRIMARY KEY,
    link_occurences int,
    total_occurences int
);

CREATE TABLE wikidumps.wikipedialinks (
    alias text PRIMARY KEY,
    pages map<text, int>
);

CREATE TABLE wikidumps.wikipediapages (
    page text PRIMARY KEY,
    aliases map<text, int>
);
