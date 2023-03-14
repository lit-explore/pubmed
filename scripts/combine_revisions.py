"""
Combine PubMed article revision batches
"""
import pandas as pd

snek = snakemake

combined = pd.read_feather(snek.input[0])

for i, infile in enumerate(snek.input[1:]):
    df = pd.read_feather(infile)
    combined = pd.concat([combined, df])

combined.reset_index(drop=True).to_feather(snek.output[0])
