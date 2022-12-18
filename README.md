# PubMed Data Preparation Pipeline

## Overview

This repo contains a [Snakemake](https://snakemake.readthedocs.io/) pipeline for downloading and
processing PubMed article data into a format that can be easily used by other efforts.

Data is acquired from the [PubMed FTP server](https://ftp.ncbi.nlm.nih.gov/pubmed/) as XML files.

These files are then parsed, and the following fields are extracted for each article:

1. ID
2. DOI
3. Title
4. Abstract
5. Date

The "ID" field in this case refers to the article's PubMed ID (PMID). In some cases, one or more of
the fields may be missing, and the pipeline can be configured to optionally exclude articles which
are missing either/both title and abstract information.

For each article, tokenization and lemmatization is performed and the results are stored separately,
so that for each article, two alternate versions of the article text are created.

Data is processed in batches, mirroring the batches used for the source article data.

So for instance, `pubmed22n0001.xml.gz` gets processed into `baseline/0001.feather` and `lemmatized/0001.feather`.

For more information about the source data, see: [PubMed Documentation](https://pubmed.ncbi.nlm.nih.gov/download/).

## Usage

To use the pipeline, first create and activate a [conda
environment](https://docs.conda.io/en/latest/) using the provided `requirements.txt` file:

```
conda create -n pubmed --file requirements.txt
conda activate pubmed
```

Next, copy the example config file, `config/config.example.yml`, and modify the config to indicate
the desired output directory to use, along with any other changes to the settings.

```
cp config/config.example.yml config/config.yml
```

Finally, launch the Snakemake pipeline, provided the config file along with any other desired
settings, e.g.:

```
snakemake -j4 --configfile config/config.yml
```

## Related

For a similar pipeline for retrieving and processing arXiv data, see: [lit-explore/arxiv](https://github.com/lit-explore/arxiv).
