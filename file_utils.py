import gzip

def generic_file_reader(path, fx):
    cont = None
    with fx(path) as f:
        cont = f.read()
    return cont

FILE_READERS = {
None : lambda x: open(x, "r"),
"gz": lambda x: gzip.open(x, "rb")
}

def getExtension(path):
    name = path.split("/")[-1]
    ext = name.split(".")
    if len(ext) > 1:
        return ext[-1]
    else:
        return None

def read_file(path):
    ext = getExtension(path)
    if (ext == None) or (ext not in FILE_READERS.keys()):
        return generic_file_reader(path, FILE_READERS[None])
    else:
        return generic_file_reader(path, FILE_READERS[ext])

