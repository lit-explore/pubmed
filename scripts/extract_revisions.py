"""
Parses Pubmed XML and extracts revision information for each article
"""
import os
import gzip
import xml.etree.ElementTree as ET
from typing import List
from datetime import datetime
import pandas as pd

snek = snakemake

with gzip.open(snek.input[0], "r") as fp:
    tree = ET.parse(fp)

root = tree.getroot()

files:List[str] = []
pmids:List[int] = []
dates:List[str] = []

# iterate over articles in xml file
for article in root.findall(".//PubmedArticle"):
    # extract pmid
    pmid = article.find(".//ArticleId[@IdType='pubmed']").text

    # exclude entries with malformed/non-numeric pubmed ids
    if pmid is not None and not pmid.isnumeric():
        continue

    # extract revision date
    rev_elem = article.find(".//DateRevised")

    if rev_elem is None:
        continue

    year = rev_elem[0].text
    month = rev_elem[1].text
    day = rev_elem[2].text

    DATE_FORMAT = "%Y %m %d" if month.isnumeric() else "%Y %b %d"

    date_str = datetime.strptime(f"{year} {month} {day}", DATE_FORMAT).isoformat()

    files.append(os.path.basename(snek.input[0]))
    pmids.append(int(pmid))
    dates.append(date_str)

dat = pd.DataFrame({"id": pmids, "file": files, "date": dates})

if dat.shape[0] == 0:
    raise Exception("No articles found with all require components!")

dat.reset_index(drop=True).to_feather(snek.output[0])
