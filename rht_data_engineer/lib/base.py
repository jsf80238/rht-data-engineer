import sys
from dataclasses import dataclass
import enum
from inspect import stack, getargvalues, currentframe, FrameInfo
import logging
import os
from pathlib import Path
import re
import sqlite3
from typing import Union, Optional, Type, Tuple
import unicodedata
# Imports above are standard Python
# Imports below are 3rd-party
import pendulum


DATABASE_LOCATION = Path.home() / "rht.db"
DATAFILE_LOCATION = Path("..") / "data"


class C(enum.StrEnum):
    BLACK_SQUARE = unicodedata.lookup("BLACK SQUARE")  # ■, x25A0
    CHAR = "CHAR"
    CLASSPATH = "CLASSPATH"
    CLASS_NAME = "class_name"
    CONNECTION_STRING = "connection_string"
    CSV_EXTENSION = ".csv"
    DATABASE = "database"
    DATE = "DATE"
    DECIMAL = "DECIMAL"
    EXCEL_EXTENSION = ".xlsx"
    FLOAT = "FLOAT"
    JAR = "jar"
    JDBC = "jdbc"
    NUMBER = "NUMBER"
    PARQUET_EXTENSION = ".parquet"
    PORT_NUMBER = "port_number"
    SQL_EXTENSION = ".sql"
    VARCHAR = "VARCHAR"


class Logger:
    __instance = None

    def __new__(cls,
                level: [str | int] = None,
                **kwargs
                ):
        """
        Return the same logger for every invocation.
        """
        if not cls.__instance:
            if level:
                cls.level = level.upper()
            else:
                cls.level = logging.INFO

            cls.logger = logging.getLogger()
            # Set overall logging level, will be overridden by the handlers
            cls.logger.setLevel(logging.DEBUG)
            # Formatting
            date_format = '%Y-%m-%dT%H:%M:%S%z'
            formatter = logging.Formatter('%(asctime)s | %(levelname)8s | %(message)s', datefmt=date_format)
            # Logging to STDERR
            console_handler = logging.StreamHandler()
            console_handler.setLevel(cls.level)
            console_handler.setFormatter(formatter)
            # Add console handler to logger
            cls.logger.addHandler(console_handler)
            cls.__instance = object.__new__(cls)
        return cls.__instance

    @classmethod
    def get_logger(cls) -> logging.Logger:
        return cls.logger

    @classmethod
    def set_level(cls, level: str) -> None:
        for handler in cls.logger.handlers:
            handler.setLevel(level)


class Database:
    """
    Wrapper around a database connection module.
    Plainly this is overkill for SQLite, but gives a general approach for an enterprise database.
    """
    __instance = None

    def __new__(cls,
                # host_name: str,
                # port_number: int,
                # database_name: str,
                # user_name: str,
                # password: str,
                # auto_commit: bool = False,
                # **kwargs
                ):
        """
        Return the same database object (connection) for every invocation.
        """
        cls.logger = Logger().get_logger()
        if not cls.__instance:
            cls.logger.info(f"Connecting to {DATABASE_LOCATION} ...")
            cls.database_connection = sqlite3.connect(DATABASE_LOCATION)
            cls.logger.info("... connected.")
            cls.__instance = object.__new__(cls)
        return cls.__instance

    @classmethod
    def get_connection(cls):
        return cls.database_connection

    @classmethod
    def execute(
            cls,
            sql: str,
            parameters: list = list(),
            is_debug: bool = False,
            ):
        """
        | Wrapper around the Cursor class
        | Returns a tuple containing:
        | 1: the cursor with the result set
        | 2: a list of the column names in the result set, or an empty list if not a SELECT statement

        :param sql: the query to be executed
        :param parameters: the parameters to fill the placeholders
        :param is_debug: if True log the query but don't do anything
        :return: a tuple containing:
        """
        # Gather information about the caller so we can log a useful message
        # Search the stack for the first file which is not this one (that will be the caller we are interested in)
        for frame_info in stack():
            if frame_info.filename != __file__:
                identification = f"From directly above line {frame_info.lineno} in file {Path(frame_info.filename).name}"
                break
        else:
            identification = "<unknown>"
        # Format the SQL to fit on one line
        # formatted_sql = re.sub(r"\s+", " ", sql).strip()
        cursor = cls.get_connection().cursor()
        # Log the statement with the parameters converted to their passed values
        sql_for_logging = sql
        pattern = re.compile(r"\s*=\s*\?")
        needed_parameter_count = pattern.findall(sql)
        if len(needed_parameter_count) != len(parameters):
            cls.logger.warning(
                f"I think the query contains {len(needed_parameter_count)} placeholders and I was given {len(parameters)} parameters: {parameters}")
        for param in parameters:
            if type(param) == str:
                param = "'" + param + "'"
            elif type(param) == int:
                param = str(param)
            else:
                cls.logger.warning("Cannot log SQL, sorry.")
                break
            sql_for_logging = re.sub(pattern, " = " + param, sql_for_logging, 1)
        # Format the SQL to fit on one line
        sql_for_logging = re.sub(r"\s+", " ", sql_for_logging).strip()
        if is_debug:
            cls.logger.info(f"{identification} would have executed: {sql_for_logging}.")
            return cursor, list()
        # We are not merely debugging, so try to execute and return results
        cls.logger.info(f"{identification} executing: {sql_for_logging} ...")
        try:
            cursor.execute(sql, parameters)
        except Exception as e:
            cls.logger.error(e)
            raise e
        # Successfully executed, now return a list of the column names
        try:
            column_list = [column[0] for column in cursor.description]
        except TypeError:  # For DML statements there will be no column description returned
            column_list = list()
            cls.logger.info(f"Rows affected: {cursor.rowcount:,d}.")
        return cursor, column_list

    @classmethod
    def fetch_one_row(
        cls,
        sql: str,
        parameters: list = list(),
        default_value=None
        ) -> Union[list, str, int]:
        """
        | Run the given query and fetch the first row.
        | If default_value not provided then ...
        | If there is only a single element in the select clause the function returns None.
        | If there are multiple elements in the select clause the function to return [None]*the number of elements.

        :param sql: the query to be executed
        :param parameters: the parameters to fill the placeholders
        :param default_value: if the query does not return any rows, return this.
        :return: if the return contains two or more things return them as a list, else return a single item.
        """
        cursor, column_list = cls.execute(sql, parameters)
        for row in cursor.fetchall():
            if len(row) == 1:
                return row[0]
            else:
                return row
            break
        cls.logger.info("No rows selected.")
        if default_value:
            return default_value
        else:
            if len(column_list) == 1:
                return None
            else:
                return [None] * len(column_list)


def dedent_sql(s):
    """
    Remove leading spaces from all lines of a SQL query.
    Useful for logging.

    :param s: query
    :return: cleaned-up version of query
    """
    return "\n".join([x.lstrip() for x in s.splitlines()])


def get_line_count(file_path: Union[str, Path]) -> int:
    """
    See https://stackoverflow.com/questions/845058/how-to-get-line-count-of-a-large-file-cheaply-in-python
    """
    f = open(file_path, 'rb')
    line_count = 0
    buf_size = 1024 * 1024
    read_f = f.raw.read

    buf = read_f(buf_size)
    while buf:
        line_count += buf.count(b'\n')
        buf = read_f(buf_size)

    return line_count


if __name__ == "__main__":
    logger = Logger().get_logger()
    logger.info("a logging message")
    mydb = Database()
    query = """
        select count(*) from sqlite_master where type='table' and name = 'repair_order'
        """
    cursor, column_list = mydb.execute(query)
    for item in cursor.description:
        print(item)
    for r in cursor.fetchall():
        # row = dict(zip(column_list, r))
        for value in r:
            print()
            print(value)
            print(type(value))
    exit()
