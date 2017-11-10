from gdc_data_processing import GDCCaseMetadataHandler, GeneExpressionQuantification
from gdc_rest import singleProjectSearchOperation
from os.path import exists
from os import makedirs

# GDC project name from which the data will be retrieved

def get_project_geq_data(projID):
    parentFolder = "C:/Users/Vitor Vieira/MEOCloud/Projectos/PhDThesis/GDC-Portal/"  # Main folder where local project data is kept
    projFolder = parentFolder + projID + "/"  # Selected GDC project folder
    geq_main_folder = projFolder + "GEQ/"  # Gene expression data folder
    geq_dfs_folder = geq_main_folder + "Datasets/"  # Folder to save the processed datasets

    if not exists(geq_dfs_folder):
        makedirs(geq_dfs_folder)

    casefields = ["annotations",
                  "demographic",
                  "diagnoses",
                  "diagnoses.treatments",
                  "exposures",
                  "family_histories",
                  "files",
                  "files.analysis",
                  "files.cases.samples",
                  "summary",
                  "tissue_source_site"]  # fields to include in the search

    casefilt = singleProjectSearchOperation(projectId=projID, dataCategory="Transcriptome Profiling")  # search filter
    mh = GDCCaseMetadataHandler(filter=casefilt, expand=casefields,
                                maxEntries=1500)  # metadata handler object that will fetch the data

    dataParams = {"analysis.workflow_type": "HTSeq - FPKM"}  # Parameters for the gene expression data search
    fh = GeneExpressionQuantification(mh, "case_id", filterparams=dataParams)


    # write both dataframes to a file
    fh.getMetadata().to_csv(projFolder + "metadata.csv") # dataframe with case metadata
    fh.getFileData().to_csv(projFolder + "GEQ_filedata.csv") # dataframe with gene expression quantification file metadata

    outputs = ["cases.samples.sample_type",
               "demographic.ethnicity",
               "demographic.race",
               "demographic.year_of_birth",
               "demographic.year_of_death",
               "diagnoses.age_at_diagnosis",
               "diagnoses.tumor_stage",
               "diagnoses.vital_status",
               "exposures.bmi"] # each field corresponds to a desired output vector


    X = fh.getDataMatrix(geq_main_folder) # assemble the gene expression matrix from local files (downloaded if missing)
    Dy = {output: fh.getOutputVector(output) for output in outputs} # dictionary linking output fields with their vectors

    # Write the gene expression matrix and output vectors to a file for later use
    X.to_csv(geq_dfs_folder + "geq_data.csv")
    [output.to_csv(geq_dfs_folder + "geq_output_" + y + ".csv") for y, output in Dy.items()]

[get_project_geq_data(projID) for projID in ["TCGA-COAD","TCGA-BRCA","TCGA-OV"]]