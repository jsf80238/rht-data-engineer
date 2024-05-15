from io import StringIO
from pathlib import Path
# Imports above are standard Python
# Imports below are 3rd-party
import pandas as pd
from xml_to_dict import XMLtoDict
import xml.etree.ElementTree as ET
# Imports below are custom
from lib.base import Logger, Database, DATAFILE_LOCATION


def iter_docs(author):
    author_attr = author.attrib
    for doc in author.iter('document'):
        doc_dict = author_attr.copy()
        doc_dict.update(doc.attrib)
        doc_dict['data'] = doc.text
        yield doc_dict

# xml_data = open("/home/jason/PycharmProjects/rht_data_engineer/data/shard_12.xml").read()
# print(xml_data)
# etree = ET.parse("/home/jason/PycharmProjects/rht_data_engineer/data/shard_12.xml") #create an ElementTree object
# print(pd.DataFrame(list(iter_docs(etree.getroot()))))
#
# exit()

# logger = Logger().get_logger()
# db_connection = Database().get_connection()


def read_files_from_dir(folder: [str, Path]) -> list[pd.DataFrame]:
    """

    :param dir: /full/path/to/dir/containing/data/files
    :return: a list of dataframes, where each dataframe represents the content of a file in the given directory
    """
    if isinstance(folder, str):
        folder = Path(str)
    file_list = sorted(folder.glob("*.xml"))
    for i, file_path in enumerate(file_list, 1):
        content = open(file_path).read()
        print(content)
        parser = XMLtoDict()
        print(parser.parse(content))
        exit()


for item in read_files_from_dir(DATAFILE_LOCATION):
    pass
