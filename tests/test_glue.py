import json
from pathlib import Path

import pytest  # noqa
from flatten.aws import GlueColumnMapping
from flatten.aws import GlueTable
from flatten.aws import ToFlatParquet


def test_duplicate_columns():
    """
    column names are not lower cased atm and just passed on to the table
    per default glue/hive is not case sensitive, so this could lead to collosions if the names
    are not escaped with double quotes
    """
    with open(
        Path(Path(__file__).parent.absolute(), "glue_table_duplicate_column.json")
    ) as f:
        table_metadata = json.load(f)
    test_table = GlueTable("test", "test", metadata=table_metadata)
    column_mapping = test_table.flat_mapping()
    assert len(set([c[1] for c in column_mapping])) == len(column_mapping)


def test_columns_mapping():
    with open(Path(Path(__file__).parent.absolute(), "glue_table.json")) as f:
        table_metadata = json.load(f)
    test_table = GlueTable("test", "test", metadata=table_metadata)

    column_mapping = test_table.flat_mapping()
    assert column_mapping == [
        GlueColumnMapping(source_name='"id"', target_name='"id"', type="string"),
        GlueColumnMapping(
            source_name='"programid"', target_name='"programid"', type="string"
        ),
        GlueColumnMapping(
            source_name='"orchestra"', target_name='"orchestra"', type="string"
        ),
        GlueColumnMapping(
            source_name='"season"', target_name='"season"', type="string"
        ),
        GlueColumnMapping(
            source_name='"concert"."eventType"',
            target_name='"concert_eventType"',
            type="string",
        ),
        GlueColumnMapping(
            source_name='"concert"."Location"',
            target_name='"concert_Location"',
            type="string",
        ),
        GlueColumnMapping(
            source_name='"concert"."Venue"',
            target_name='"concert_Venue"',
            type="string",
        ),
        GlueColumnMapping(
            source_name='"concert"."Date"', target_name='"concert_Date"', type="string"
        ),
        GlueColumnMapping(
            source_name='"concert"."Time"', target_name='"concert_Time"', type="string"
        ),
        GlueColumnMapping(
            source_name='"work"."ID"', target_name='"work_ID"', type="string"
        ),
        GlueColumnMapping(
            source_name='"work"."composerName"',
            target_name='"work_composerName"',
            type="string",
        ),
        GlueColumnMapping(
            source_name='"work"."workTitle"',
            target_name='"work_workTitle"',
            type="string",
        ),
        GlueColumnMapping(
            source_name='"work"."conductorName"',
            target_name='"work_conductorName"',
            type="string",
        ),
        GlueColumnMapping(
            source_name='"work"."soloists"."soloistName"',
            target_name='"work_soloists_soloistName"',
            type="string",
        ),
        GlueColumnMapping(
            source_name='"work"."soloists"."soloistInstrument"',
            target_name='"work_soloists_soloistInstrument"',
            type="string",
        ),
        GlueColumnMapping(
            source_name='"work"."soloists"."soloistRoles"',
            target_name='"work_soloists_soloistRoles"',
            type="string",
        ),
    ]


def test_generated_sql():
    with open(Path(Path(__file__).parent.absolute(), "glue_table.json")) as f:
        table_metadata = json.load(f)
    test_table = GlueTable("test", "test", metadata=table_metadata)
    flat_table = ToFlatParquet(
        database="test",
        source_table=test_table,
        target_table=test_table,
        target_table_location="test",
        workgroup="test",
        s3_staging_dir="test",
    )
    # TODO move to file
    expected = 'create table "test"."test" with (external_location = \'s3://xxxxx/nyphilarchive/\' , format = \'PARQUET\' , parquet_compression = \'SNAPPY\') as select "id" as "id" , "programid" as "programid" , "orchestra" as "orchestra" , "season" as "season" , "concert"."eventType" as "concert_eventType" , "concert"."Location" as "concert_Location" , "concert"."Venue" as "concert_Venue" , "concert"."Date" as "concert_Date" , "concert"."Time" as "concert_Time" , "work"."ID" as "work_ID" , "work"."composerName" as "work_composerName" , "work"."workTitle" as "work_workTitle" , "work"."conductorName" as "work_conductorName" , "work"."soloists"."soloistName" as "work_soloists_soloistName" , "work"."soloists"."soloistInstrument" as "work_soloists_soloistInstrument" , "work"."soloists"."soloistRoles" as "work_soloists_soloistRoles" from "test"."test"'  # noqa
    assert " ".join(
        flat_table.generate_insert_overwrite_query(test_table).split()
    ) == " ".join(expected.split())
