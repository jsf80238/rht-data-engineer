import logging
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path

import pandas as pd
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
class RepairOrderDetail:
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


TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"
logger = Logger().get_logger()


def read_files_from_dir(folder: [str, Path]) -> list[str]:
    """

    :param folder: /full/path/to/dir/containing/data/files
    :return: a list of dataframes, where each dataframe represents the content of a file in the given directory
    """
    if isinstance(folder, str):
        folder = Path(str)
    file_list = sorted(folder.glob("*.xml"))
    logger.info(f"Reading from folder '{folder}' ...")
    for i, file_path in enumerate(file_list, 1):
        logger.info(f"({i:>2} of {len(file_list)}) reading '{file_path}' ...")
        yield open(file_path).read()


def parse_xml(content: str) -> dict:
    """

    :param content: this is the XML read from the data file
    :return: that XML in the form of a dictionary
    """
    parser = XMLtoDict()
    return parser.parse(content)


def create_target_tables() -> None:
    """
    Create target tables idempotently
    """
    mydb = Database()

    sql = "select count(*) from sqlite_master where type='table' and name = 'repair_order'"
    the_count = mydb.fetch_one_row(sql)
    if not the_count:
        sql = """
            create table repair_order (
                order_id integer,
                timestamp text,
                status text,
                cost real,
                technician text,
                primary key (order_id)
            )
        """
        mydb.execute(sql)

    sql = "select count(*) from sqlite_master where type='table' and name = 'repair_order_detail'"
    the_count = mydb.fetch_one_row(sql)
    if not the_count:
        sql = """
            create table repair_order_detail (
                order_id integer,
                part_name text,
                quantity int,
                primary key (order_id, part_name),
                foreign key (order_id) references repair_order(order_id)
            )
        """
        mydb.execute(sql)

    for table_name in "repair_order_detail", "repair_order":
        mydb.execute(f"delete from {table_name}")


repair_order_dict = dict()  # keys are order ID, values are the repair order
repair_order_detail_dict = dict()  # keys are order ID, values are part/quantity tuples

for file_content in read_files_from_dir(DATAFILE_LOCATION):
    temp_list = list()
    try:
        order_raw = parse_xml(file_content)[Tag.EVENT.value]
        order_id = int(order_raw[Tag.ORDER_ID.value])
        status = order_raw[Tag.STATUS.value]
        cost = float(order_raw[Tag.COST.value])
        technician = order_raw[Tag.REPAIR_DETAILS.value][Tag.TECHNICIAN.value]
        parts = order_raw[Tag.REPAIR_DETAILS.value][Tag.REPAIR_PARTS.value][Tag.PART.value]
        if isinstance(parts, dict):
            parts = [parts]
        for part in parts:
            part_item = RepairOrderDetail(
                order_id=order_id,
                part_name=part[Tag.NAME.value],
                quantity=part[Tag.QUANTITY.value],
            )
            temp_list.append(part_item)
        timestamp = order_raw[Tag.DATE_TIME.value]
        timestamp_as_datetime = datetime.strptime(timestamp, TIMESTAMP_FORMAT)
    except Exception as e:
        logger.error(f"Skipping because of: {e}")
        continue
    if order_id in repair_order_dict:
        logger.debug(f"Will replace data for order {order_id}.")
        existing_timestamp = repair_order_dict[order_id].timestamp
    else:
        logger.debug(f"We are seeing order {order_id} for the first time.")
        existing_timestamp = datetime(1, 1, 1)  # Earliest possible date
    if timestamp_as_datetime > existing_timestamp:
        repair_order_dict[order_id] = RepairOrder(
            order_id=order_id,
            timestamp=timestamp_as_datetime,
            status=status,
            cost=cost,
            technician=technician,
        )
        repair_order_detail_dict[order_id] = temp_list.copy()

# To increase the speed of insertion convert the dictionaries to dataframes
new_dict = {k: asdict(v) for k, v in repair_order_dict.items()}
repair_order_df = pd.DataFrame()
create_target_tables()

# Create the order records, then the order detail records
