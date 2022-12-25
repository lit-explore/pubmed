"""
Combine PubMed article revision batches
"""
import pandas as pd

combined = pd.read_feather(snakemake.input[0])

for i, infile in enumerate(snakemake.input[1:]):
    df = pd.read_feather(infile)
    combined = pd.concat([combined, df])

combined.reset_index(drop=True).to_feather(snakemake.output[0])
