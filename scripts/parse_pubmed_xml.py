"""
Parses Pubmed XML data and extract article pmid, title, and abstract information
"""
import gzip
import sys
from datetime import datetime
from typing import List
import xml.etree.ElementTree as ET
import pandas as pd

# assign "snakemake" to another variable to avoid excessive warnings
snek = snakemake

# load article revision info
revisions = pd.read_feather(snek.input[1])

# load article xml
with gzip.open(snek.input[0], "r") as fp:
    tree = ET.parse(fp)

root = tree.getroot()

pmids:List[int] = []
dois:List[str] = []
dates:List[str] = []
titles:List[str] = []
abstracts:List[str] = []

# max token length
MAX_LEN = snek.config['tokens']['max_len']

def extract_pub_date(article):
    """
    Extracts publication date for a single article
    """
    # extract publication date;
    try:
        date_elem = article.find(".//PubDate")

        if date_elem is None:
            raise Exception("Publication date not found! skipping..")

        year_elem = date_elem.find("Year")

        if year_elem is not None:
            year = year_elem.text

        month_elem = date_elem.find("Month")

        if month_elem is not None:
            month = month_elem.text
        else:
            # if no month found, default to january
            month = "01"

        day_elem = date_elem.find("Day")

        if day_elem is not None:
            day = day_elem.text
        else:
            # if no day found, default to the 1st
            day = "01"

        # check for numeric/string months
        date_format = "%Y %m %d" if month.isnumeric() else "%Y %b %d"
        date_str = datetime.strptime(f"{year} {month} {day}", date_format).isoformat()
    except:
        # if date parsing fails, just leave field blank
        date_str = ""

    return date_str

def extract_revision_date(article):
    """
    Extracts revision date for a single article
    """
    # extract revision date
    rev_elem = article.find(".//DateRevised")

    if rev_elem is None:
        return None

    year = rev_elem[0].text
    month = rev_elem[1].text
    day = rev_elem[2].text

    DATE_FORMAT = "%Y %m %d" if month.isnumeric() else "%Y %b %d"

    date_str = datetime.strptime(f"{year} {month} {day}", DATE_FORMAT).isoformat()

    return date_str

# iterate over articles in xml file
for article in root.findall(".//PubmedArticle"):
    # extract title
    title_elem = article.find(".//ArticleTitle")

    if title_elem is None or title_elem.text is None:
        title = ""
    else:
        title = title_elem.text.replace('\n', ' ').strip()

    #  if snek.config['exclude_articles']['missing_title'] and title == "":
    if title == "":
        continue

    # extract abstract
    abstract_elem = article.find(".//AbstractText")

    if abstract_elem is None or abstract_elem.text is None:
        abstract = ""
    else:
        abstract = abstract_elem.text.replace('\n', ' ').strip()

    #  if snek.config['exclude_articles']['missing_abstract'] and abstract == "":
    if abstract == "":
        continue

    # extract pmid
    pmid_elem = article.find(".//ArticleId[@IdType='pubmed']")

    if pmid_elem is not None:
        # exclude any entries with malformed/non-numeric pubmed ids; only one such id encountered so
        # far: "17181r22""
        if not pmid_elem.text.isnumeric():
            continue

        pmid = int(pmid_elem.text)
    else:
        continue

    # if the article has already been included, skip;
    # this should only occur when multiple revisions are made for the same article on the same day,
    # in which case, the first entry encountered will be kept.

    # in the future, it may be good to modify the revision checking logic to all track the revision
    # "version" so that these can be differentiated and the most recent version retained.
    if pmid in pmids:
        continue

    # extract doi
    doi_elem = article.find(".//ArticleId[@IdType='doi']")

    if doi_elem is None or doi_elem.text is None:
        doi = ""
    else:
        doi = doi_elem.text.lower()

    # extract publication date
    pub_date = extract_pub_date(article)

    # if multiple revisions for an article are present, only keep the most recent one
    revs = revisions[revisions.id == pmid]

    if revs.shape[0] > 1:
        last_rev = revs.sort_values('date').tail(1).date.values[0]
        rev_date = extract_revision_date(article)

        if last_rev != rev_date:
            continue

    # remove excessively long tokens
    title = " ".join([x for x in title.split() if len(x) <= MAX_LEN])
    abstract = " ".join([x for x in abstract.split() if len(x) <= MAX_LEN])

    # replace underscores with HTML-encoded representations to simplify downstream handling of
    # n-gram tokens
    title = title.replace("_", "%5f")
    abstract = abstract.replace("_", "%5f")

    pmids.append(pmid)
    dois.append(doi)
    titles.append(title)
    abstracts.append(abstract)
    dates.append(pub_date)

dat = pd.DataFrame({"id": pmids, "doi": dois, "title": titles, "abstract": abstracts, "date": dates})

if dat.shape[0] == 0:
    print(f"Skipping {snek.input[0]}: no articles found with all needed pieces...")
    sys.exit()

dat.reset_index(drop=True).to_feather(snek.output[0])
