from sqlalchemy import Table, create_engine, MetaData, Column, String
from ceneton_database_tools.fmp_reader import FMPSchema, sanitize_name

def make_core_table(table_name: str,
                    metadata: MetaData,
                    fields: list[str],
                    mapping: dict[str,str] = None) -> Table:
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

def create_database_and_schema(db_url: str, schema: FMPSchema, table_name: str | None = None, mapping: dict[str, str] | None = None):
    engine   = create_engine(db_url)
    metadata = MetaData()

    if table_name is None:
        table_name = sanitize_name(schema.name)

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

    metadata.create_all(engine)

    return engine, metadata, core_table


