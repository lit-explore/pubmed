"""
lit-explore: PubMed Data Preparation
"""
import os
from os.path import join

configfile: "config/config.yml"

# pubmed annual file numbers
pubmed_annual = [f"{n:04}" for n in range(config['pubmed']['annual_start'], 
                                          config['pubmed']['annual_end'] + 1)]

# pubmed update file numbers
pubmed_updates = [f"{n:04}" for n in range(config['pubmed']['updates_start'], 
                                          config['pubmed']['updates_end'] + 1)]

# exclude pubmed22n0654.xml.gz (missing abstracts for all entries)
if config['exclude_articles']['missing_abstract']:
    pubmed_annual = [x for x in pubmed_annual if x != "0654"]

batches = pubmed_annual + pubmed_updates

rule all:
    input:
        join(config["out_dir"], "corpus/raw.feather"),
        join(config["out_dir"], "corpus/lemmatized.feather"),
        join(config["out_dir"], "revisions.feather")

rule combine_pubmed_articles:
    input:
        expand(join(config["out_dir"], "raw/{pubmed_num}.feather"), pubmed_num=batches)
    output:
        join(config["out_dir"], "corpus/raw.feather")
    script:
        "scripts/combine_articles.py"

rule combine_pubmed_lemmatized_articles:
    input:
        expand(join(config["out_dir"], "lemmatized/{pubmed_num}.feather"), pubmed_num=batches)
    output:
        join(config["out_dir"], "corpus/lemmatized.feather")
    script:
        "scripts/combine_articles.py"

rule create_lemmatized_pubmed_corpus:
    input:
        join(config["out_dir"], "raw/{pubmed_num}.feather")
    output:
        join(config["out_dir"], "lemmatized/{pubmed_num}.feather")
    resources:
        load=40
    script:
        "scripts/lemmatize_text.py"

rule parse_pubmed_xml:
    input: 
        join(config["out_dir"], "xml/pubmed22n{pubmed_num}.xml.gz"),
        join(config["out_dir"], "revisions.feather")
    output:
        join(config["out_dir"], "raw/{pubmed_num}.feather")
    script:
        "scripts/parse_pubmed_xml.py"

rule combine_revisions:
    input:
        expand(join(config["out_dir"], "revisions/{pubmed_num}.feather"), pubmed_num=batches)
    output:
        join(config["out_dir"], "revisions.feather")
    script:
        "scripts/combine_revisions.py"

rule extract_revisions:
    input: 
        join(config["out_dir"], "xml/pubmed22n{pubmed_num}.xml.gz")
    output:
        join(config["out_dir"], "revisions/{pubmed_num}.feather")
    script:
        "scripts/extract_revisions.py"

rule download_pubmed_data:
    output:
        join(config["out_dir"], "xml/pubmed22n{pubmed_num}.xml.gz")
    resources:
        load=50
    shell:
        """
        cd `dirname {output}`

        if [ "{wildcards.pubmed_num}" -gt "{config[pubmed][annual_end]}" ]; then
            echo "daily!";
            ftpdir="updatefiles"
        else
            echo "annual!"
            ftpdir="baseline"
        fi

        echo "https://ftp.ncbi.nlm.nih.gov/pubmed/${{ftpdir}}/pubmed22n{wildcards.pubmed_num}.xml.gz"
        wget "https://ftp.ncbi.nlm.nih.gov/pubmed/${{ftpdir}}/pubmed22n{wildcards.pubmed_num}.xml.gz"
        """

# vi:syntax=snakemake
