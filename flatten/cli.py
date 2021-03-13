import typer
from flatten.aws import GlueTable
from flatten.aws import ToFlatParquet


def main(
    database: str = typer.Argument(..., help="The name of glue database"),
    source_table: str = typer.Argument(
        ..., help="The name of the (nested) source table"
    ),
    target_table: str = typer.Argument(
        ..., help="The name of the flattend target table"
    ),
    target_table_location: str = typer.Argument(
        ..., help="The s3 location for the data of the flattend table"
    ),
    s3_staging_dir: str = typer.Argument(
        ...,
        help="The s3 location where athena is allowed to write its query results",
    ),
    workgroup: str = typer.Option(
        "primary", help="The athena workspace, if you use them"
    ),
):
    # TODO add a check if logged into AWS CLI
    source_table = GlueTable(
        database_name=database,
        table_name=source_table,
    )
    target_table = GlueTable(database_name=database, table_name=target_table)
    ToFlatParquet(
        database=database,
        source_table=source_table,
        target_table=target_table,
        target_table_location=target_table_location,
        workgroup=workgroup,
        s3_staging_dir=s3_staging_dir,
    ).insert_overwrite()


def cli():
    typer.run(main)
