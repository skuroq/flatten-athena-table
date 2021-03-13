from __future__ import annotations

import os
import random
import string
from functools import cached_property
from typing import Dict
from typing import List
from typing import NamedTuple
from urllib.parse import urlsplit

import boto3
import pyathena
import sqlparse
from botocore.exceptions import ClientError
from flatten.hive_parser import HiveParser
from flatten.hive_parser import reconstruct_array
from flatten.utils import column_query_path_format
from flatten.utils import flatten_dict
from jinja2 import Template
from loguru import logger
from pkg_resources import resource_filename
from pyathena.cursor import Cursor

s3 = boto3.resource("s3")


def splitted_s3_key(s3_url: str) -> Dict:
    (_, netloc, path, _, _) = urlsplit(s3_url, allow_fragments=False)

    parts = {
        "bucket": netloc,
        "path": "/".join(path.split("/")[1:]),
    }
    return parts


def query_gen(template: str, query_args: Dict) -> str:
    """
    Uses jinja2 to render sql templates
    :param template:
    :param query_args:
    :return:
    """
    with open(template) as f:
        query_template = f.read()

    query = Template(query_template).render(query_args)
    formatted_query = sqlparse.format(
        query,
        comma_first=True,
        reindent=True,
        keyword_case="lower",
        strip_comments=True,
    )
    return formatted_query


class AthenaConnection:
    """
    Base class for Athena queries.
    Provides methods to execute a query against Athena.
    """

    def __init__(self, database, workgroup, s3_staging_dir, cursor_class=Cursor):
        self.database = database
        self.workgroup = workgroup
        self.s3_staging_dir = s3_staging_dir
        self.cursor_class = cursor_class

    def query(self, sql: str):
        logger.info("{}".format(sql))
        conn = pyathena.connect(
            work_group=self.workgroup,
            s3_staging_dir=self.s3_staging_dir,
            cursor_class=self.cursor_class,
        )
        cursor = conn.cursor()
        cursor.execute(sql.rstrip(";") + ";")
        return cursor


class GlueColumnMapping(NamedTuple):
    source_name: str
    target_name: str
    type: str


class GlueTable:
    glue_client = boto3.client("glue")
    hive_parser = HiveParser()

    def __init__(self, database_name, table_name, metadata=None, table_version_id=None):
        self.database_name = database_name
        self.table_name = table_name
        self.table_version_id = table_version_id
        if metadata:
            self.metadata = metadata

    @cached_property
    def metadata(self):
        if self.exists():
            metadata = self._get_table(
                client=self.glue_client,
                database=self.database_name,
                table_name=self.table_name,
                table_version_id=self.table_version_id,
            )
        return metadata

    @property
    def full_name(self):
        return f'"{self.database_name}"."{self.table_name}"'

    @staticmethod
    def _get_table(client, database, table_name, table_version_id):
        try:
            if table_version_id:
                response = client.get_table_version(
                    DatabaseName=database,
                    TableName=table_name,
                    VersionId=table_version_id,
                )
                return response["TableVersion"]["Table"]
            else:
                response = client.get_table(DatabaseName=database, Name=table_name)
                return response["Table"]
        except client.exceptions.EntityNotFoundException:
            raise ValueError(f"Glue Table {table_name} not found")

    def purge_data(self) -> int:
        s3_url_parts = splitted_s3_key(self.location())
        bucket = s3.Bucket(s3_url_parts["bucket"])
        return bucket.objects.filter(Prefix=s3_url_parts["path"]).delete()

    def exists(self) -> bool:
        try:
            self.glue_client.get_table(
                DatabaseName=self.database_name, Name=self.table_name
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                return False

    def location(self):
        return self.metadata["StorageDescriptor"]["Location"]

    def columns(self) -> List[tuple]:
        """Get Colums from Glue-Crawler-Data-Store
        Returns:
            List[tuple]: [description]
        """
        glue_cols = self.metadata["StorageDescriptor"]["Columns"]
        columns = []
        for c in glue_cols:
            columns.append((c["Name"], c["Type"]))

        # Check if there are columns that are the same if lowercased
        lowercased = [s[0].lower() for s in columns]
        if len(lowercased) != len(set(lowercased)):
            raise ValueError(
                "Please check your table schema for duplicate column names (upper/lower case)!"
            )
        return columns

    def delete(self):

        return self.glue_client.delete_table(
            DatabaseName=self.database_name, Name=self.table_name
        )

    @property
    def flat_columns(self):
        return [(col.target_name, col.type) for col in self.flat_mapping()]

    def flat_mapping(self) -> List[GlueColumnMapping]:
        """Flattens columns with complex types (structs,maps)
        into multiple flat type columns
        Returns:
            List[GlueColumnMapping]: [description]
        """
        column_mapping = []
        # TODO refactor nested logic
        for col_name, col_type in self.columns():
            parsed_type = self.hive_parser(col_type)
            if isinstance(parsed_type, dict):
                source_columns = flatten_dict(
                    {col_name: parsed_type}, format_func=column_query_path_format
                )
                target_columns = flatten_dict({col_name: parsed_type})
                for source_column, (target_column, target_type) in zip(
                    source_columns, target_columns.items()
                ):
                    if isinstance(target_type, list):
                        target_type = reconstruct_array(
                            self.hive_parser.parser, target_type
                        )
                    column_mapping.append(
                        GlueColumnMapping(source_column, target_column, target_type)
                    )
            else:
                if isinstance(col_type, list):
                    col_type = reconstruct_array(self.hive_parser.parser, col_type)
                # Wrapping col_name in double quotes because hive and presto have different
                # constraints on which character are allowed
                column_mapping.append(
                    GlueColumnMapping(f"{col_name}", f"{col_name}", col_type)
                )

        return column_mapping

    def create(
        self,
        columns,
        location,
        partitions=None,
        input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        serde="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        parameters=None,
    ):
        """
        Creates a table in Glue. The default settings create a parquet table
        :param columns: Column definition of the form [("name", "type"), ("name", "type"), ...]
        :param location: Place where the table is stored, e.g., a path in S3
        :param partitions: Partition columns, e.g.  [("name", "type"), ("name", "type"), ...]
        :param input_format: Format in which the table handles input (e.g., Parquet)
        :param output_format: Format in which the table handles output (e.g., Parquet)
        :param serde: Serde for storing rows
        :param parameters: Dictionary of additional parameters
        :return:
        """
        """
        Force reloading metadata by deleting it as there may be auto-generated info in
        there that will change
        """
        try:
            delattr(self, "metadata")
        except AttributeError:
            logger.debug("Attribute Metadata was not accessed before")
        # TODO consider moving the following creation of the glue table config into a separate jinja template
        if not parameters:
            parameters = {
                "classification": "parquet",
                "typeOfData": "file",
                "has_encrypted_data": "false",
                "EXTERNAL": "TRUE",
            }

        if not partitions:
            partitions = []
        serde_info = {
            "SerializationLibrary": serde,
        }
        if serde == "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe":
            serde_info["Parameters"] = {"serialization.format": "1"}

        return self.glue_client.create_table(
            DatabaseName=self.database_name,
            TableInput={
                "Name": self.table_name,
                "Owner": "hadoop",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": name, "Type": type_} for name, type_ in columns
                    ],
                    "Location": location,
                    "SkewedInfo": {
                        "SkewedColumnNames": [],
                        "SkewedColumnValues": [],
                        "SkewedColumnValueLocationMaps": {},
                    },
                    "Parameters": {},
                    "BucketColumns": [],
                    "InputFormat": input_format,
                    "OutputFormat": output_format,
                    "NumberOfBuckets": -1,
                    "SerdeInfo": serde_info,
                },
                "PartitionKeys": [
                    {"Name": name, "Type": type_} for name, type_ in partitions
                ],
                "Parameters": parameters,
                "TableType": "EXTERNAL_TABLE",
            },
        )


class ToFlatParquet:
    def __init__(
        self,
        database,
        source_table,
        target_table,
        target_table_location,
        workgroup,
        s3_staging_dir,
        source_table_version_id=None,
    ):
        self.s3_staging_dir = s3_staging_dir
        self.workgroup = workgroup
        self.database = database
        self.source_table = source_table
        self.target_table = target_table
        self.target_table_location = target_table_location
        self.source_table_version_id = source_table_version_id

        self.conn = AthenaConnection(
            database=self.database,
            workgroup=self.workgroup,
            s3_staging_dir=self.s3_staging_dir,
        )

    def refresh_target_table(self, drop_if_exists=False) -> None:
        if drop_if_exists:
            logger.info("Removing old target table data from glue.")
            self.target_table.delete()
        if self.target_table.exists():
            logger.info("Removing old target table data  s3.")
            self.target_table.purge_data()

        if not self.target_table.exists():
            logger.info(f"Creating target table {self.target_table.full_name}.")
            self.target_table.create(
                columns=[
                    (col.target_name, col.type)
                    for col in self.source_table.flat_mapping()
                ],
                location=self.target_table_location,
            )

    def generate_insert_overwrite_query(self, tmp_table):
        return query_gen(
            template=resource_filename(
                __name__, os.path.join("create_flat_tmp_table_parquet.sql")
            ),
            query_args={
                "tmp_table": tmp_table.full_name,
                "source_tb_name": self.source_table.full_name,
                "location": self.target_table.location(),
                "columns": [
                    (col.source_name, col.target_name)
                    for col in self.source_table.flat_mapping()
                ],
            },
        )

    def insert_overwrite(self, temp_db=None) -> None:
        """
        This functions first creates a temporary table with a s3 location equal to the "target" table for the
        specified partition.
        Existing files in the target table are removed in the refresh_target_table().
        :param temp_db:
        :return:
        """
        assert self.source_table.columns() is not None

        self.refresh_target_table()
        logger.info("Creating temporary table for data transformation.")
        if not temp_db:
            temp_db = self.database

        temp_table_name = "".join(random.choices(string.ascii_uppercase, k=10))
        temp_table_glue = GlueTable(temp_db, temp_table_name)

        if temp_table_glue.exists():
            temp_table_glue.delete()

        self.conn.query(self.generate_insert_overwrite_query(temp_table_glue))
        logger.info("Deleting temporary table.")
        temp_table_glue.delete()
        logger.info(f"Successfully flattend {self.source_table.full_name}!")
        logger.info(
            f"You can find the flattend table in athena {self.target_table.full_name}"
        )
