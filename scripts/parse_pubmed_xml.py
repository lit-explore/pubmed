"""
Parses Pubmed XML data and extract article pmid, title, and abstract information
"""
import gzip
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime

with gzip.open(snakemake.input[0], "r") as fp:
    tree = ET.parse(fp)

root = tree.getroot()

pmids = []
dois = []
dates = []
titles = []
abstracts = []

# max token length
MAX_LEN = snakemake.config['tokens']['max_len']

# TESTING
import os
if os.path.basename(snakemake.input[0]) == "pubmed22n0654.xml.gz":
    breakpoint()

# iterate over articles in xml file
for article in root.findall(".//PubmedArticle"):
    # extract title
    title_elem = article.find(".//ArticleTitle")

    if title_elem is None or title_elem.text is None:
        title = ""
    else:
        title = title_elem.text.replace('\n', ' ').strip()

    if snakemake.config['exclude_articles']['missing_title'] and title == "":
        continue

    # extract abstract
    abstract_elem = article.find(".//AbstractText")

    if abstract_elem is None or abstract_elem.text is None:
        abstract = ""
    else:
        abstract = abstract_elem.text.replace('\n', ' ').strip()

    if snakemake.config['exclude_articles']['missing_abstract'] and abstract == "":
        continue

    # extract pmid
    pmid = article.find(".//ArticleId[@IdType='pubmed']").text

    # exclude any entries with malformed/non-numeric pubmed ids; only one such id encountered so
    # far: "17181r22""
    if pmid is not None and not pmid.isnumeric():
        continue

    # extract doi
    doi_elem = article.find(".//ArticleId[@IdType='doi']")

    if doi_elem is None or doi_elem.text is None:
        doi = ""
    else:
        doi = doi_elem.text.lower()

    try:
        date_elem = article.find(".//PubDate")

        year = date_elem[0].text
        month = date_elem[1].text

        # if not day specified, default to the 1st
        if len(date_elem) == 3:
            day = date_elem[2].text
        else:
            day = "01"

        # check for numeric/string months
        date_format = "%Y %m %d" if month.isnumeric() else "%Y %b %d"

        date_str = datetime.strptime(f"{year} {month} {day}", date_format).isoformat()
    except:
        # if date parsing fails, just leave field blank
        date_str = ""

    # remove excessively long tokens
    title = " ".join([x for x in title.split() if len(x) <= MAX_LEN])
    abstract = " ".join([x for x in abstract.split() if len(x) <= MAX_LEN])

    pmids.append(pmid)
    dois.append(doi)
    titles.append(title)
    abstracts.append(abstract)
    dates.append(date_str)

dat = pd.DataFrame({"id": pmids, "doi": dois, "title": titles, "abstract": abstracts, "date": dates})

if dat.shape[0] == 0:
    raise Exception("No articles found with all require components!")

dat.reset_index(drop=True).to_feather(snakemake.output[0])
