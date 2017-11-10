import operator

import pandas as pd
from io import StringIO
from numpy import where
from functools import reduce
from gdc_rest import RequestEndpoint, http_post


def unfold_dataframe(df, identifier, meta, sep="."):
    #    if not (True in df[identifier].isnull()):
    #        data = json_normalize(df, record_path=identifier, meta=meta)
    #    else:
    data = pd.DataFrame.from_records([x if isinstance(x, dict) else {} for x in df[identifier].tolist()])
    data.index = df.index
    data.columns = [str(identifier) + sep + str(name) for name in data.columns]
    if not isinstance(data, pd.DataFrame):
        return data
    else:
        return pd.concat([df, data], axis=1)


def unlist_singles(iterable, parent=None, index=None):
    if isinstance(iterable, dict):
        for key in iterable:
            unlist_singles(iterable[key], iterable, key)
    elif isinstance(iterable, list):
        if len(iterable) == 1:
            if parent is None:
                pass
                # print("iterable is already unlisted.")
            else:
                parent[index] = iterable[0]
                unlist_singles(iterable[0], parent=parent, index=index)
        elif len(iterable) > 1:
            for i in range(len(iterable)):
                unlist_singles(iterable[i], iterable, i)
    else:
        return iterable


OPERATORS = {"and": operator.and_, "or": operator.or_, "is": operator.is_, "not": operator.not_}


def dataframe_filter(dataframe, operator_function, conditions):
    truthvalues = [dataframe[k] == v for k, v in iter(conditions.items())]
    intersect = list(set(where(reduce(operator_function, truthvalues))[0]))
    if len(intersect) > 0:
        return dataframe.iloc[intersect,]
    else:
        raise Exception("No columns were selected. Filtering resulted in an empty DataFrame")


class LayeredDataframe():
    def __init__(self, dataframe):
        self.data = dataframe
        self.__update_indexes()

    def unfold(self, meta):
        for k in self.dictids:
            self.data = unfold_dataframe(self.data, k, meta).drop([k], axis=1)
        self.__update_indexes()

    def __update_indexes(self):
        self.listids = self.data.columns[where(self.data.apply(lambda x: list in [type(i) for i in x]))]
        self.dictids = self.data.columns[where(self.data.apply(lambda x: dict in [type(i) for i in x]))]

    def extract(self, identifier, metacolumn):
        records = self.data[identifier].tolist()
        frames = [pd.DataFrame(records[i], index=[i] * len(records[i])) for i in range(len(records))]
        dataframe = pd.concat([pd.concat(frames, axis=0), self.data[metacolumn]], join="outer", axis=1)
        dataframe.index = list(range(dataframe.shape[0]))
        return dataframe

class GDCProjectData(object):
    def __init__(self):
        request = RequestEndpoint(RequestEndpoint.PROJECTS,format="tsv",size=1000,fields="*")
        data = request.request(http_post, convert=False)
        self.data = pd.read_csv(StringIO(data.decode("utf-8")), sep="\t")
        self.data.index = self.data["project_id"]

    def getProjectNames(self):
        return self.data["project_id"]

    def getAvailableFields(self):
        return self.data.columns

    def getProjectInformation(self, projectNames):
        return self.data.loc[projectNames,:]

class GDCCaseMetadataHandler(object):
    def __init__(self, filter=None, fields="*", expand=None, maxEntries=10000):
        params = {
            "format": "json",
            "size": maxEntries,
            "pretty": "false",
            "fields": ','.join(fields) if isinstance(fields, list) else fields, }

        if filter is not None:
            params["filters"] = filter
        if expand is not None:
            params["expand"] = ','.join(expand) if isinstance(expand, list) else expand

        self.req = RequestEndpoint(endpoint=RequestEndpoint.CASES, **params)
        self.__frame = self.fetch()
        self.__frame.unfold("case_id")

    def getData(self):
        return self.__frame.data

    def fetch(self):
        print("Retrieving case/file metadata")
        data = self.req.request(http_post, convert=True)
        unlist_singles(data)
        print("Data retrieval is now complete")
        return LayeredDataframe(pd.DataFrame(data))

    def getBranch(self, identifier, metacolumn):
        df = self.__frame.extract(identifier, metacolumn)
        # df.index = self.__frame.data[metacolumn]
        return df


class GDCFileData(object):
    filterparams = None

    def __init__(self, handler, metacolumn="case_id", fileidcolumn="file_id", filterparams=None):
        self.handler = handler
        self.metacolumn = metacolumn
        self.fileidcolumn = fileidcolumn
        self.dataindex = None
        if filterparams is not None:
            if self.filterparams is None:
                self.filterparams = filterparams
            else:
                self.filterparams.update(filterparams)
        ld = LayeredDataframe(self.handler.getBranch(identifier="files", metacolumn=self.metacolumn))
        ld.unfold(fileidcolumn)
        data = ld.data

        if self.filterparams is not None:
            data = dataframe_filter(data, OPERATORS["and"], self.filterparams)

        self.__dataframe = LayeredDataframe(data)
        self.__dataframe.unfold(fileidcolumn)
        self.updateIndex()
        self.updateMetadata()

    def updateMetadata(self):
        # df = pd.concat([self.getFileData(), self.handler.getData().drop(["files"], axis=1)],
        #                keys=[self.metacolumn],
        #                axis=1,
        #                ignore_index=False)
        df = pd.merge(self.getFileData(), self.handler.getData().drop(["files"], axis=1), how="left", on=self.metacolumn)
        self.__metadata = df
        self.metafeatures = self.__metadata.columns
        print("Metadata for the requested cases has been updated")

    def getFileData(self):
        return self.__dataframe.data

    def filterByCaseMetadata(self, d, operator="and", update=False):
        cases = dataframe_filter(self.handler.getData(), OPERATORS[operator], d)
        df = self.__dataframe.data[self.__dataframe.data[self.metacolumn].apply(lambda x: x in cases[self.metacolumn])]
        if update:
            self.__dataframe.data = df
        else:
            return df
        self.updateIndex()
        self.updateMetadata()

    def filterByFileData(self, d, operator="and", update=False):
        df = dataframe_filter(self.__dataframe.data, OPERATORS[operator], d)
        if update:
            self.__dataframe.data = df
        else:
            return df
        self.updateIndex()
        self.updateMetadata()

    def confirmLocalStorage(self, path, download=True, file_name_col="file_name"):
        fd = self.getFileData()
        self.handler.req.downloadFromTSVMetadata(fd, writeFolder=path)

    def updateIndex(self):
        fileids = self.getFileData()[self.fileidcolumn]
        caseids = self.getFileData()[self.metacolumn]
        idx = pd.MultiIndex(
            levels=[tuple(fileids.tolist()), tuple(caseids.tolist())],
            names=[self.fileidcolumn, self.metacolumn],
            labels=[list(range(len(fileids))), list(range(len(caseids)))])
        print("Sample index has been updated")
        self.dataindex = idx

    def getMetadata(self):
        return self.__metadata

    def getOutputVector(self, column):
        col = self.getMetadata()[column]
        col.index = self.dataindex
        return col

class GeneExpressionQuantification(GDCFileData):
    filterparams = {
        "access": "open",
        "data_type": "Gene Expression Quantification"
    }

    # Function to read files with FPKM counts and convert into a dataframe
    def parse_file(self, path, verbose=False):
        if verbose:
            print("Reading file: ", path)
        return pd.read_csv(path, sep='\t', header=None, index_col=0)

    def __getDataFrameFromFiles(self, filepath):
        filelist = self.getFileData()["file_name"].tolist()
        df = pd.concat((self.parse_file(filepath + path, False) for path in filelist), axis=1).T
        return df

    def getDataMatrix(self, filepath, caseMetaColumn="case_id", fileIdColumn="file_name", removeEnsemblRevisionId=True):
        assert isinstance(filepath, str), "File path must be a string."
        self.confirmLocalStorage(filepath)

        df = self.__getDataFrameFromFiles(filepath)
        df.index = self.dataindex
        if removeEnsemblRevisionId:
            df.columns = [f.split(".")[0] for f in df.columns]
        return df




def filter_by_output_vector(X, y, outputs, outputType="class"):
    if outputType == "class":
        samples = y.isin(outputs)
    elif outputType == "reg":
        samples = y.apply(outputs)
    return X[samples], y[samples]

def read_dataset_from_file(path):
    return pd.read_csv(path,index_col = [0,1])

def read_output_vector_from_file(path):
    return pd.read_csv(path, index_col = [0,1], squeeze = True, header=None)