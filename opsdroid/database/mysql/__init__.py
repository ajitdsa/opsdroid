# -*- coding: utf-8 -*-
"""A module for opsdroid to allow persist in postgres database."""
import logging
import json
import asyncio
import aiomysql

from contextlib import asynccontextmanager
from opsdroid.database import Database
from opsdroid.helper import JSONEncoder, JSONDecoder
from voluptuous import Any, Required

_LOGGER = logging.getLogger(__name__)
CONFIG_SCHEMA = {
    "host": str,
    "port": Any(int, str),
    "database": str,
    "user": str,
    Required("password"): str,
    "table": str,
}


def create_table_if_not_exists(func):
    """Decorator to check if the table specified exists.
    Creates table if it does not exist"""

    async def create_table_query(connection, table):
        cursor = await connection.cursor()
        async with connection.cursor() as cursor:
            # Create table if it does not exist
            await cursor.execute(
                f"SHOW TABLES LIKE '{table}'"
            )
            if not cursor.fetchone().result():
                await cursor.execute(
                    f"CREATE TABLE {table} ( `key`  VARCHAR(256), `data` JSON DEFAULT NULL, PRIMARY KEY (`key`) )"
                )

    async def wrapper(*args, **kwargs):
        # args[0].connection will get DatabaseMysql.connection
        connection = args[0].connection
        table = args[0].table

        if " " in table:
            _LOGGER.warning(
                'table contains a space character. Suggest changing "'
                + table
                + '" to "'
                + table.strip(" ")
                + '"'
            )

        try:
            await create_table_query(connection, table)
            return await func(*args, **kwargs)
        except Exception:
            _LOGGER.error("MySQL Could not create table %s", table)
            return None

    return wrapper


def check_table_format(func):
    """Decorator to check the format of the table for proper fields.
    Creates table if it does not exist"""

    async def get_table_structure_query(connection, table):
        cursor = await connection.cursor()
        async with connection.cursor() as cursor:
            # Check Table's data structure is correct
            await cursor.execute(
                f"SELECT `column_name`, `data_type` FROM information_schema.columns WHERE table_name = '{table}'"
            )
            return await cursor.fetchall()

    async def wrapper(*args, **kwargs):
        # args[0].connection will get DatabaseMysql.connection
        connection = args[0].connection
        table = args[0].table

        try:
            data_structure = await get_table_structure_query(connection, table)

            key_column = [
                x
                for x in data_structure
                if x[0] == "key" and x[1] == "varchar"
            ][0]
            data_column = [
                x
                for x in data_structure
                if x[0] == "data" and x[1] == "longtext"
            ][0]

            if key_column and data_column:
                _LOGGER.info(
                    "PostgresSQL table %s verified correct data structure", table
                )
            return await func(*args, **kwargs)
        except Exception:
            _LOGGER.error("PostgresSQL table %s has incorrect data structure", table)
            return None

    return wrapper


class DatabaseMysql(Database):
    """A module for opsdroid to allow memory to persist in a MySQL database."""

    def __init__(self, config, opsdroid=None):
        """Create the connection.

        Args:
            config (dict): The config for this database specified in the
                           `configuration.yaml` file.
            opsdroid (OpsDroid): An instance of opsdroid.core.

        """
        super().__init__(config, opsdroid=opsdroid)
        _LOGGER.debug("Loaded MySQL database connector.")
        self.name = "mysql"
        self.config = config
        self.loop = asyncio.get_event_loop()
        self.connection = None
        self.table = self.config.get("table", "opsdroid")

    async def connect(self):
        """Connect to the database."""
        host = self.config.get("host", "127.0.0.1")
        port = self.config.get("port", 3306)
        database = self.config.get("database", "opsdroid")
        user = self.config.get("user", "opsdroid")
        pwd = self.config.get("password")
        self.connection = await aiomysql.connect(
            user=user, password=pwd, db=database, host=host, port=port, loop=self.loop, autocommit=True
        )
        _LOGGER.info("Connected to MySQL.")

    async def disconnect(self):
        """Disconnect from the database."""
        if self.connection:
            await self.connection.close()
            _LOGGER.info("Disconnected from MySQL.")

    @create_table_if_not_exists
    @check_table_format
    async def put(self, key, data):
        """Insert or replace an object into the database for a given key.

        Args:
            key (str): the key is the key field in the table.
            data (object): the data to be inserted or replaced

        """

        _LOGGER.debug("Putting %s into MySQL table %s", key, self.table)
        if isinstance(data, str):
            data = {"value": data}
        json_data = json.dumps(data, cls=JSONEncoder)
        await self.put_query(key, json_data)

    async def put_query(self, key, json_data):
        """SQL transaction to write data to the specified table"""
        cursor = await self.connection.cursor()
        async with self.connection.cursor() as cursor:
            key_already_exists = await self.get(key)
            if key_already_exists:
                await cursor.execute(
                    f"UPDATE {self.table} SET `data` = '{json_data}' WHERE `key` = '{key}'"
                )
            else:
                await cursor.execute(
                    f"INSERT INTO {self.table} (`key`, `data`) VALUES ('{key}', '{json_data}')"
                )

    @check_table_format
    async def get(self, key):
        """Get a document from the database (key).

        Args:
            key (str): the key is the key field in the table.

        """

        _LOGGER.debug("Getting %s from MySQL table %s", key, self.table)

        values = await self.get_query(key)

        if len(values) > 1:
            _LOGGER.error(
                str(len(values))
                + " entries with same key name in PostgresSQL table %s. Only one allowed.",
                self.table,
            )
            return None

        if (len(values) == 1) and values[0][0]:
            data = json.loads(values[0][0], object_hook=JSONDecoder())
            if data.keys() == {"value"}:
                data = data["value"]
            return data

        return None

    async def get_query(self, key):
        """SQL transaction to get data from the specified table"""
        cursor = await self.connection.cursor()
        async with self.connection.cursor() as cursor:
            await cursor.execute(
                f"SELECT `data` FROM {self.table} WHERE `key` = '{key}'"
            )
            return cursor.fetchall().result()

    @check_table_format
    async def delete(self, key):
        """Delete a record from the database (key).

        Args:
            key (str): the key is the key field in the table.

        """

        _LOGGER.debug("Deleting %s from MySQL table %s.", key, self.table)
        await self.delete_query(key)

    async def delete_query(self, key):
        """SQL transaction to delete data from the specified table"""
        cursor = await self.connection.cursor()
        async with self.connection.cursor() as cursor:
            await cursor.execute(
                f"DELETE FROM {self.table} WHERE `key` = '{key}'"
            )

    @asynccontextmanager
    async def memory_in_table(self, table):
        """Use the specified table rather than the default."""
        db_copy = DatabaseMysql(self.config, self.opsdroid)
        try:
            await db_copy.connect()
            db_copy.table = table
            yield db_copy
        finally:
            await db_copy.disconnect()
