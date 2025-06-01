import datetime

from sqlalchemy import (
    Column,
    DateTime,
    MetaData,
    String,
    Table,
    create_engine,
    insert,
    inspect,
)

from ceneton_database_utils.fmp_reader import FMPSchema, sanitize_name


def make_core_table(
    table_name: str,
    metadata: MetaData,
    fields: list[str],
    mapping: dict[str, str] = None,
) -> Table:
    """
    - table_name: e.g. "my_rows"
    - fields: your sanitized field names
    - mapping (optional): maps clean_name → original_name
    """
    cols = []
    for name in fields:
        # you can attach the original header as a comment or in info
        column_args = {}
        if mapping:
            column_args["comment"] = mapping[name]
            # or: column_args["info"] = {"orig": mapping[name]}
        cols.append(Column(name, String, **column_args))

    return Table(table_name, metadata, *cols)


def create_metadata_table(table_name: str, metadata: MetaData) -> Table:
    """Create a metadata table for storing key-value metadata."""
    metadata_table_name = f"{table_name}_metadata"
    return Table(
        metadata_table_name,
        metadata,
        Column("key", String(255), primary_key=True),
        Column("value", String(1000)),
        Column("created_at", DateTime),
    )


def create_database_and_schema(
    db_url: str,
    schema: FMPSchema,
    table_name: str | None = None,
    mapping: dict[str, str] | None = None,
    db_metadata: dict[str, str] | None = None,
):
    engine = create_engine(db_url)
    metadata = MetaData()

    if table_name is None:
        table_name = sanitize_name(schema.name)

    # Check if table already exists
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    if table_name in existing_tables:
        raise ValueError(f"Table '{table_name}' already exists in the database")

    metadata_table_name = f"{table_name}_metadata"
    if metadata_table_name in existing_tables:
        raise ValueError(
            f"Metadata table '{metadata_table_name}' already exists in the database"
        )

    fields = [n for n in schema.name_mapping.keys()]
    if mapping is None:
        name_mapping = {n: schema.name_mapping[n].name for n in fields}
    else:
        name_mapping = mapping

    core_table = make_core_table(
        table_name=table_name,
        metadata=metadata,
        fields=fields,
        mapping=name_mapping,
    )

    # Create metadata table
    metadata_table = create_metadata_table(table_name, metadata)

    metadata.create_all(engine)

    # Insert metadata
    creation_time = datetime.datetime.now(datetime.timezone.utc)
    metadata_entries = [
        {
            "key": "created_at",
            "value": creation_time.isoformat(),
            "created_at": creation_time,
        },
        {
            "key": "source_schema_name",
            "value": schema.name,
            "created_at": creation_time,
        },
        {
            "key": "record_count",
            "value": str(schema.record_count),
            "created_at": creation_time,
        },
    ]

    # Add custom metadata if provided
    if db_metadata:
        for key, value in db_metadata.items():
            metadata_entries.append(
                {"key": key, "value": value, "created_at": creation_time}
            )

    with engine.begin() as conn:
        conn.execute(insert(metadata_table), metadata_entries)

    return engine, metadata, core_table
