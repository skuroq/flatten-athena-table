import json
from pathlib import Path

import pytest  # noqa
from flatten.aws import GlueColumnMapping
from flatten.aws import GlueTable
from flatten.aws import ToFlatParquet


def test_duplicate_columns():
    """
    Column names are not lower cased atm and just passed on to glue.
    Per default glue/hive is not case sensitive, so this could lead to collosions, so we check for it
    """
    with open(
        Path(Path(__file__).parent.absolute(), "glue_table_duplicate_column.json")
    ) as f:
        table_metadata = json.load(f)
    test_table = GlueTable("test", "test", metadata=table_metadata)
    with pytest.raises(ValueError):
        test_table.flat_mapping()


def test_columns_mapping():
    with open(Path(Path(__file__).parent.absolute(), "glue_table.json")) as f:
        table_metadata = json.load(f)
    test_table = GlueTable("test", "test", metadata=table_metadata)

    column_mapping = test_table.flat_mapping()
    assert column_mapping == [
        GlueColumnMapping(source_name="id", target_name="id", type="string"),
        GlueColumnMapping(
            source_name="programid", target_name="programid", type="string"
        ),
        GlueColumnMapping(
            source_name="orchestra", target_name="orchestra", type="string"
        ),
        GlueColumnMapping(source_name="season", target_name="season", type="string"),
        GlueColumnMapping(
            source_name='"concert"."eventType"',
            target_name="concert_eventtype",
            type="string",
        ),
        GlueColumnMapping(
            source_name='"concert"."Location"',
            target_name="concert_location",
            type="string",
        ),
        GlueColumnMapping(
            source_name='"concert"."Venue"', target_name="concert_venue", type="string"
        ),
        GlueColumnMapping(
            source_name='"concert"."Date"', target_name="concert_date", type="string"
        ),
        GlueColumnMapping(
            source_name='"concert"."Time"', target_name="concert_time", type="string"
        ),
        GlueColumnMapping(
            source_name='"work"."ID"', target_name="work_id", type="string"
        ),
        GlueColumnMapping(
            source_name='"work"."composerName"',
            target_name="work_composername",
            type="string",
        ),
        GlueColumnMapping(
            source_name='"work"."workTitle"',
            target_name="work_worktitle",
            type="string",
        ),
        GlueColumnMapping(
            source_name='"work"."conductorName"',
            target_name="work_conductorname",
            type="string",
        ),
        GlueColumnMapping(
            source_name='"work"."soloists"."soloistName"',
            target_name="work_soloists_soloistname",
            type="string",
        ),
        GlueColumnMapping(
            source_name='"work"."soloists"."soloistInstrument"',
            target_name="work_soloists_soloistinstrument",
            type="string",
        ),
        GlueColumnMapping(
            source_name='"work"."soloists"."soloistRoles"',
            target_name="work_soloists_soloistroles",
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
    expected = 'create table "test"."test" with (external_location = \'s3://xxxxx/nyphilarchive/\'\n                               ,  format = \'PARQUET\'\n                               ,  parquet_compression = \'SNAPPY\') as\nselect id as id\n     ,  programid as programid\n     ,  orchestra as orchestra\n     ,  season as season\n     ,  "concert"."eventType" as concert_eventtype\n     ,  "concert"."Location" as concert_location\n     ,  "concert"."Venue" as concert_venue\n     ,  "concert"."Date" as concert_date\n     ,  "concert"."Time" as concert_time\n     ,  "work"."ID" as work_id\n     ,  "work"."composerName" as work_composername\n     ,  "work"."workTitle" as work_worktitle\n     ,  "work"."conductorName" as work_conductorname\n     ,  "work"."soloists"."soloistName" as work_soloists_soloistname\n     ,  "work"."soloists"."soloistInstrument" as work_soloists_soloistinstrument\n     ,  "work"."soloists"."soloistRoles" as work_soloists_soloistroles\nfrom "test"."test"'  # noqa
    assert " ".join(
        flat_table.generate_insert_overwrite_query(test_table).split()
    ) == " ".join(expected.split())
