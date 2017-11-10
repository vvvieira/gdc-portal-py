import pandas as pd

hgnc = pd.read_csv("resources/non_alt_loci_set.txt", sep = "\t", dtype=str).astype(str).applymap(lambda x: x if (not x == "nan") else None)

def convertGeneName(name, format_from, format_to):
    return hgnc[hgnc[format_from] == name][format_to]

def drop_GEQ_genes(X,genelist):
    gns = hgnc.apply(lambda x: x.isin(genelist), axis=0).apply(any, axis=1)
    names = hgnc["ensembl_gene_id"][gns].dropna()
    return X[names]
