import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
# Imports above are standard Python
# Imports below are 3rd-party
from xml_to_dict import XMLtoDict
# Imports below are custom
from lib.base import Logger, Database, DATAFILE_LOCATION


class Tag(StrEnum):
    EVENT = "event"
    ORDER_ID = "order_id"
    DATE_TIME = "date_time"
    STATUS = "status"
    COST = "cost"
    REPAIR_DETAILS = "repair_details"
    TECHNICIAN = "technician"
    REPAIR_PARTS = "repair_parts"
    PART = "part"
    NAME = "@name"
    QUANTITY = "@quantity"


class Status(StrEnum):
    COMPLETED = "Completed"
    IN_PROGRESS = "In Progress"
    RECEIVED = "Received"
    REOPENED = "Reopened"


@dataclass
class RepairParts:
    order_id: int
    part_name: str
    quantity: int


@dataclass
class RepairOrder:
    order_id: int
    timestamp: datetime
    status: Status
    cost: float
    technician: str
    repair_parts_id: int  # key to repair_parts table 


TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"
logger = Logger().get_logger()
# db_connection = Database().get_connection()


def read_files_from_dir(folder: [str, Path]) -> list[str]:
    """

    :param dir: /full/path/to/dir/containing/data/files
    :return: a list of dataframes, where each dataframe represents the content of a file in the given directory
    """
    if isinstance(folder, str):
        folder = Path(str)
    file_list = sorted(folder.glob("*.xml"))
    logger.info(f"Reading from folder '{folder}' ...")
    for i, file_path in enumerate(file_list, 1):
        logger.info(f"Reading '{file_path}' ({i} of {len(file_list)} ...")
        yield open(file_path).read()


def parse_xml(content: str) -> dict:
    """

    :param content:
    :return:
    """
    parser = XMLtoDict()
    return parser.parse(content)


repair_order_dict = dict(RepairOrder)  # keys are order ID
repair_parts_dict = dict(RepairParts)  # keys are order ID

for file_content in read_files_from_dir(DATAFILE_LOCATION):
    try:
        order_raw = parse_xml(file_content)[Tag.EVENT.value]
    except Exception as e:
        logger.error(f"Skipping because of: {e}")
    order_id = order_raw[Tag.ORDER_ID.value]
    try:
        timestamp = order_raw[Tag.DATE_TIME.value]
        timestamp_as_datetime = datetime.strptime(timestamp, TIMESTAMP_FORMAT)
        print(timestamp_as_datetime)
    except Exception as e:
        logger.error(f"Skipping because could not parse timestamp: {e}")
    try:
        existing_timestamp = repair_order_dict[order_id].timestamp
    except:
        existing_timestamp = datetime(1, 1, 1)  # Earliest possible date
        logger.info(f"We are seeing order {order_id} for the first time.")
    if timestamp_as_datetime > existing_timestamp:
        pass
