import requests
import json
from json import loads
import pandas as pd
from io import StringIO
from os.path import exists
import hashlib
from file_utils import read_file


to_json = json.JSONEncoder().encode
from_json = json.JSONDecoder().decode
BASE_URL = "https://api.gdc.cancer.gov/"

class Fields(object):
    CASE = read_file("resources/fields/casefields.txt").split("\n")[:-1]


def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def validate_response(response, raw=False):
    if response.ok:
        if raw:
            return response
        else:
            return response.content.decode('utf-8')
    else:
        response.raise_for_status()


def http_post(url, params=None, json=None, headers=None, stream=False):
    response = requests.post(url, data=params, json=json,
                             headers=headers, stream=stream)
    return validate_response(response, stream)


def http_get(url, params=None, stream=False):
    response = requests.get(url, params=params, stream=stream)
    return validate_response(response, stream)


def stream_to_file(request, path, chunk_size=1024):
    with open(path, "wb") as f:
        for chunk in request.iter_content(chunk_size):
            f.write(chunk)
    return path


def download_from_stream(link, path, httpfun, retries=3, md5sum=None):
    tries = 0
    done = False
    while (tries < retries) and not done:
        try:
            response = httpfun(url=link, stream=True)
            stream_to_file(response, path)
        except Exception as e:
            tries = tries + 1
            print("Retrying... unable to download/save.", tries, "- Error:", e.message)
            continue

        done = md5(path) == md5sum and exists(path)
        if not done:
            tries = tries + 1
            print("Retrying... unable to validate MD5 checksum", tries, "- Error:", e.message)

    return done


def json_convert(x):
    return loads(x)['data']['hits']


class FieldValuePair(object):
    '''
    Object representing a field and corresponding values associated with it.
    Used with Operation objects to specify values to match on specific queries.
    '''

    def __init__(self, field, value):
        self.field = field
        self.value = value

    def toDict(self):
        return {"field": self.field, "value": self.value}

    def toJSON(self):
        return to_json(self.toDict())


class Operation(object):
    '''
    Operations define conditions for queries included in the filter parameter.
    Constructor parameters:

        operator: string representing a given operator. Must be included
        in Operation.SINGLE_OPERANDS or Operation.MULTIPLE_OPERANDS

        operands: always a list of Operation or FieldValuePair instances.
        In its current stage, this argument can have any length, regardless
        of operator compatibility. Caution is advised so that the amount and
        type of the operands comply with the chosen operator.
    '''
    SINGLE_OPERANDS = ("=", "!=", "<", "<=", ">", ">=", "is", "not")
    MULTIPLE_OPERANDS = ("in", "exclude", "and", "or")

    def __init__(self, operator, operands):
        self.__operator = operator
        self.__operands = operands

    def toJSON(self):
        '''
        Convert Operation of JSON payload
        '''
        return to_json(self.toDict())

    def toDict(self):
        '''
        Generate a dictionary with the structure required for creating
        JSON content
        '''
        cont = None
        if len(self.__operands) > 1:
            cont = [operand.toDict() for operand in self.__operands]
        else:
            cont = self.__operands[0].toDict()
        d = {
            "op": self.__operator,
            "content": cont}
        return d

    def __repr__(self):
        return


def simple_op(field, op, value):
    return Operation(op, operands=[FieldValuePair(
        field, value)])


class RequestEndpoint(object):
    '''
    Class containing methods for using the GDC REST API.
    Constructor parameters:
        endpoint: A string specifying the REST endpoint from which the data
        will be accessed

        Other arguments can be passed as avaliable by the REST API. Thus far,
        the keys of the RequestEndpoint.PARAM_CONFIG dictionary are validated
        and supported.
    '''
    PARAM_CONFIG = {
        "from": int,
        "size": int,
        "sort": str,
        "pretty": str,
        "filters": Operation,
        "format": str,
        "fields": str,
        "expand": str,
        "facets": str}

    OUTPUT_FUNC = {
        "tsv": lambda x: pd.read_csv(StringIO(str(x)), sep="\t"),
        # "json": lambda x: from_json(x)["data"]["hits"]
        "json": json_convert}
    FILES = "files"
    CASES = "cases"
    PROJECTS = "projects"
    ANNOTATIONS = "annotations"
    MAPPING = "files/_mapping"
    DOWNLOAD = "data/"

    def __init__(self, endpoint, **kwargs):
        self.endpoint = endpoint
        self.url = BASE_URL + self.endpoint
        # self.reqfun = lambda x: f(url=self.url, data=x)
        self.params = {}
        if not (self.validateParameters()):
            raise Exception("Invalid parameter type!")
        for kw in kwargs:
            if kw in RequestEndpoint.PARAM_CONFIG:
                if kw == "filters":
                    self.params[kw] = kwargs[kw].toJSON()
                else:
                    self.params[kw] = kwargs[kw]
        if "format" not in self.params:
            self.params["format"] = "json"

    def validateParameters(self):
        '''
        Validates the dictionary containing the parameters. Returns a boolean
        that is true if the parameters are correctly typed
        '''
        valid = not (False in [self.params[k] in (
            None, RequestEndpoint.PARAM_CONFIG[k]) for k in self.params])
        return valid

    def params_to_JSON(self):
        '''
        Converts the parameter dictionary to JSON
        '''
        return to_json(self.params)

    def request(self, f, params=None, convert=True):
        '''
        Connects to the endpoint and returns the response.
        Parameters:
            f: A function yielding a response's content (http_get or http_post)
            params: The dictionary containing the query's parameters
            convert: Defines whether the result will be converted according
            to its format or kept as text
        '''
        if params == None:
            params = self.params
        content = f(self.url, params)
        if convert:
            return RequestEndpoint.OUTPUT_FUNC[params["format"]](content)
        else:
            return content

    def downloadFile(self, file_id, file_name, httpfun, md5sum=None, writeFolder="", skipIfExists=True, retries=3):
        '''
        Downloads a single file and writes it
        Parameters:
            file_id - UUID of the file in the GDC portal
            file_name - Name of the file (used for saving)
            httpfun - http_get or http_post
            writeFolder - folder where the file will be written
        '''
        link = BASE_URL + RequestEndpoint.DOWNLOAD + str(file_id)
        if exists(writeFolder + file_name):
            chksm = md5(writeFolder + file_name)
            valid = md5sum == chksm
            if not valid:
                print("MD5 checksum for", file_name, "does not match:", md5sum, "!=", chksm)
                status = download_from_stream(link, writeFolder + file_name, httpfun, md5sum=md5sum, retries=retries)
                if status:
                    print("\tCorrupted file redownloaded successfully!")
            elif valid and skipIfExists:
                pass
                #print("Skipping", writeFolder + file_name)

            else:
                print("Overwriting valid file:", file_name)
                download_from_stream(link, writeFolder + file_name, httpfun, md5sum=md5sum, retries=retries)
        else:
            download_from_stream(link, writeFolder + file_name, httpfun, md5sum=md5sum, retries=retries)
            print("Successfully downloaded ", file_name)

    def downloadFromTSVMetadata(self, request, colnm_file_id="file_id", colnm_file_name="file_name", httpfun=http_get,
                                writeFolder="", skipIfExists=True):
        '''
        Downloads multiple files with UUIDs from previously requested TSV metadata
        to the GDC servers
        '''
        return [self.downloadFile(x[1][colnm_file_id], x[1][colnm_file_name], httpfun, x[1]["md5sum"], writeFolder,
                                  skipIfExists, 3) for x in
                request[[colnm_file_id, colnm_file_name, "md5sum"]].iterrows()]

    def downloadFromJSONMetadata(self, request, colnm_file_id="file_id", colnm_file_name="file_name", httpfun=http_get,
                                 writeFolder="", skipIfExists=True):
        '''
        Downloads multiple files with UUIDs from previously requested JSON metadata
        '''
        return [
            self.downloadFile(r[colnm_file_id], r[colnm_file_name], httpfun, r["md5sum"], writeFolder, skipIfExists, 3)
            for r in request]


def singleProjectSearchOperation(projectId, dataCategory):
    return Operation("and", operands=[
        simple_op("project.project_id", "=", [projectId]),
        simple_op("files.access", "=", ['open']),
        simple_op("files.data_category", "=", dataCategory)])