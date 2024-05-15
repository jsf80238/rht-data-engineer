import argparse
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
# Imports above are standard Python
# Imports below are 3rd-party
import pandas as pd
from xml_to_dict import XMLtoDict
# Imports below are custom
from lib.base import Logger, Database


class Tag(StrEnum):
    """
    Avoids typos
    """
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
    """
    Identifies valid values
    """
    COMPLETED = "Completed"
    IN_PROGRESS = "In Progress"
    RECEIVED = "Received"
    REOPENED = "Reopened"


@dataclass
class RepairOrderDetail:
    """
    This is the child object
    """
    order_id: int
    part_name: str
    quantity: int


@dataclass
class RepairOrder:
    """
    This is the parent object
    """
    order_id: int
    timestamp: datetime
    status: Status
    cost: float
    technician: str


DEFAULT_DATAFILE_LOCATION = Path("..") / "data"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"


def read_files_from_dir(folder: [str, Path]) -> list[str]:
    """
    Read XML content.
    :param folder: /full/path/to/dir/containing/data/files, or an equivalent Path object
    :return: yield strings, where each string represents the content of a file in the given directory
    """
    return_list = list()  # Could also use yield
    if isinstance(folder, str):
        folder = Path(folder)
    file_list = sorted(folder.glob("*.xml"))
    logger.info(f"Reading from folder '{folder}' ...")
    for i, file_path in enumerate(file_list, 1):
        logger.info(f"({i:>2} of {len(file_list)}) reading '{file_path}' ...")
        data = open(file_path).read()
        return_list.append(data)
    return return_list


def parse_xml(content: str) -> dict:
    """
    Convert XML to form suitable for analysis and insertion.
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Populated the repair order tables.')

    parser.add_argument('--data-dir',
                        metavar="/path/to/datafiles",
                        default=DEFAULT_DATAFILE_LOCATION,
                        help=f"Default is {DEFAULT_DATAFILE_LOCATION}. Only XML files supported.")
    logging_group = parser.add_mutually_exclusive_group()
    logging_group.add_argument('--verbose', action='store_true')
    logging_group.add_argument('--terse', action='store_true')

    args = parser.parse_args()
    datafile_location = args.data_dir
    if args.verbose:
        logger = Logger("DEBUG").get_logger()
    elif args.terse:
        logger = Logger("WARNING").get_logger()
    else:
        logger = Logger().get_logger()

    # Had trouble using pandas.read_xml so used instead: https://pypi.org/project/xml-to-dict/
    repair_order_dict = dict()  # keys are order ID, values are the repair order
    repair_order_detail_dict = dict()  # keys are order ID, values are part/quantity tuples

    for file_content in read_files_from_dir(datafile_location):
        temp_list = list()
        try:
            order_raw = parse_xml(file_content)[Tag.EVENT]
            order_id = int(order_raw[Tag.ORDER_ID])
            status = order_raw[Tag.STATUS]
            cost = float(order_raw[Tag.COST])
            technician = order_raw[Tag.REPAIR_DETAILS][Tag.TECHNICIAN]
            parts = order_raw[Tag.REPAIR_DETAILS][Tag.REPAIR_PARTS][Tag.PART]
            if isinstance(parts, dict):
                parts = [parts]
            for part in parts:
                part_item = RepairOrderDetail(
                    order_id=order_id,
                    part_name=part[Tag.NAME],
                    quantity=part[Tag.QUANTITY],
                )
                temp_list.append(part_item)
            timestamp = order_raw[Tag.DATE_TIME]
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
    # and use Pandas' built-in to_sql() method.
    # And yes, we earlier put this data into the RepairOrder and RepairOrderDetail classes
    # only then to extract it into ordinary dictionaries.
    # That was unnecessary, but it's a habit of mine because the dataclasses _generally_ make for more readable code.
    repair_order_df = pd.DataFrame.from_records([asdict(v) for v in repair_order_dict.values()])
    repair_order_detail_list = list()
    for v in repair_order_detail_dict.values():
        for line_item in v:
            repair_order_detail_list.append(asdict(line_item))
    repair_order_detail_df = pd.DataFrame.from_records(repair_order_detail_list)

    create_target_tables()
    mydb = Database()
    # Insert the order records ...
    try:
        table_name = "repair_order"
        df = repair_order_df
        df.to_sql(table_name, mydb.get_connection(), if_exists="append", index=False)
        logger.info(f"Inserted {df.shape[0]} rows into {table_name}.")
    except Exception as e:
        logger.error(e)
        exit(1)
    # ... then the order detail records
    try:
        table_name = "repair_order_detail"
        df = repair_order_detail_df
        df.to_sql(table_name, mydb.get_connection(), if_exists="append", index=False)
        logger.info(f"Inserted {df.shape[0]} rows into {table_name}.")
    except Exception as e:
        logger.error(e)
        exit(1)
