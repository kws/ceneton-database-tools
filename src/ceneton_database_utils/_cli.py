import bz2
import csv
import hashlib
import json
import sys
from contextlib import contextmanager
from io import BytesIO, TextIOWrapper
from pathlib import Path
from typing import Generator

import click
from tqdm import tqdm

from ceneton_database_utils.fmp_reader import FMPSchema, read_fmp_xml
from ceneton_database_utils.sql import create_database_and_schema


@click.group()
def cli():
    pass


@contextmanager
def _open_xml_stream(file_path: str) -> Generator[TextIOWrapper, None, None]:
    file_path = Path(file_path)
    if file_path.is_dir():
        files = sorted(file_path.glob("*.bz2"))
        file_path = files[-1]

    if file_path.suffix == ".bz2":
        with bz2.open(file_path, "rb") as f:
            yield file_path, f
    else:
        with open(file_path, "rb") as f:
            yield file_path, f


@cli.command
@click.argument("file_path", type=click.Path(exists=True))
def dump_headers(file_path: str):
    """Dump the headers of the FMP XML file."""
    with _open_xml_stream(file_path) as (file_path, source):
        stream = read_fmp_xml(source)
        schema: FMPSchema | None = None
        for record in stream:
            if isinstance(record, FMPSchema):
                schema = record
                break

        csv_writer = csv.writer(sys.stdout)
        csv_writer.writerow(["original_name", "clean_name"])
        for key, field in schema.name_mapping.items():
            csv_writer.writerow([field.name, key])


@cli.command(name="print")
@click.argument("file_path", type=click.Path(exists=True))
def print_stream(file_path: str):
    """Print the stream of records from the FMP XML file."""

    with _open_xml_stream(file_path) as (file_path, source):
        stream = read_fmp_xml(source)
        stream = tqdm(stream)
        for record in stream:
            if isinstance(record, FMPSchema):
                print(f"Parsing {record.record_count} records from {record.name}")
                stream.total = record.record_count
        print("Done")


@cli.command()
@click.argument("file_path", type=click.Path(exists=True))
def to_json(file_path: str):
    """Convert the FMP XML file to JSONL (JSON Lines) format."""
    with _open_xml_stream(file_path) as (file_path, source):
        stream = read_fmp_xml(source)
        stream = tqdm(stream)
        for record in stream:
            if isinstance(record, FMPSchema):
                stream.total = record.record_count
            else:
                json_str = json.dumps(record._asdict())
                print(json_str)


def _read_mapping(mapping_column: str | None) -> dict[str, str] | None:
    if mapping_column is None:
        return None

    if ":" in mapping_column:
        mapping_file, mapping_column = mapping_column.split(":")
    else:
        mapping_file = mapping_column
        mapping_column = 1

    mapping = {}
    with open(mapping_file, "r") as f:
        reader = csv.reader(f)
        header = next(reader)
        if isinstance(mapping_column, str):
            mapping_column = header.index(mapping_column)
        for row in reader:
            mapping[row[1]] = row[mapping_column]

    return mapping


@cli.command()
@click.argument("file_path", type=click.Path(exists=True))
@click.argument("db_url", type=str)
@click.option("--mapping-column", type=str, help="Column name to use for mapping")
@click.option("--table-name", type=str, help="Name of the table to create")
@click.option(
    "--db-metadata",
    multiple=True,
    help="Metadata to store as key=value pairs (can be repeated)",
)
def create_db(
    file_path: str,
    db_url: str,
    mapping_column: str,
    table_name: str,
    db_metadata: tuple[str],
):
    """Create a database from FMP XML file.

    Args:
        file_path: Path to the FMP XML file to process
        db_url: SQLAlchemy database URL. Examples:
            - sqlite:///mydata.db
            - postgresql+psycopg2://user:pass@host:5432/dbname
            You can also set the DB_URL environment variable.
    """
    mapping = _read_mapping(mapping_column)

    # Parse metadata key=value pairs
    metadata_dict = {}
    for item in db_metadata:
        if "=" not in item:
            raise click.BadParameter(
                f"Metadata must be in key=value format, got: {item}"
            )
        key, value = item.split("=", 1)
        metadata_dict[key.strip()] = value.strip()

    with _open_xml_stream(file_path) as (file_path, source):
        content = source.read()
        content_sha = hashlib.sha256(content).hexdigest()
        metadata_dict["source_url"] = file_path.absolute().resolve().as_posix()
        metadata_dict["source_sha256"] = content_sha

        stream = read_fmp_xml(BytesIO(content))
        database_schema = next(stream)
        if not isinstance(database_schema, FMPSchema):
            raise ValueError("First element is not a FMPSchema")

        engine, metadata, core_table = create_database_and_schema(
            db_url,
            database_schema,
            mapping=mapping,
            table_name=table_name,
            db_metadata=metadata_dict,
        )
        stream = tqdm(stream, total=database_schema.record_count)

        BATCH_SIZE = 500
        batch: list[dict] = []

        with engine.begin() as conn:
            for record in stream:
                batch.append(record._asdict())

                if len(batch) >= BATCH_SIZE:
                    # bulk‐insert this batch
                    conn.execute(core_table.insert(), batch)
                    batch.clear()

            # any leftovers?
            if batch:
                conn.execute(core_table.insert(), batch)
