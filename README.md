# Ceneton Database Tools

This repository provides tools for processing and converting the **Ceneton** database from Leiden University's FileMaker Pro format into more accessible formats. The Ceneton database is a comprehensive census of Dutch-language theatrical works up to the year 1803, developed and maintained by the Department of Dutch at Leiden University.

## About Ceneton

**Ceneton** ("Census Nederlands Toneel") is a major scholarly resource documenting over 12,500 Dutch plays from the 16th to early 19th century. The database includes:

- **12,500+ Dutch plays** with detailed bibliographic records
- Author and original author information  
- Title (formal and full)
- Publication info: year, place, printer/publisher
- Genre, number of acts, list of characters
- Performance setting: place and time
- Incipits and transcribed title pages
- Fingerprints and collations for bibliographic identification
- **First editions and reprints** — some printed over 100 times
- **Facsimiles and full texts** of 1,300+ plays
- Holdings from over 40 European libraries

While the official Ceneton database is distributed via *FileMaker Pro*, a commercial and proprietary platform, this toolkit aims to make the material more usable for researchers, developers, and educators by providing tools to convert the data into open formats.

## Related Repositories

This toolkit is part of a larger ecosystem of Ceneton archival projects:

- **[ceneton-database](https://github.com/kws/ceneton-database)** - Archival copy of the complete Ceneton dataset in XML format
- **[ceneton-texts](https://github.com/kws/ceneton-texts)** - Archival copy of Ceneton transcripts with version control and unique IDs
- **[ceneton-texts-utils](https://github.com/kws/ceneton-texts-utils)** - Utilities for maintaining the transcript archive

## Installation

Install the package directly from GitHub using pip:

```bash
pip install git+https://github.com/kws/ceneton-database-tools
```

## Usage

After installation, the toolkit provides a command-line interface accessible via the `ct-db` command. The tools work directly with the bz2-compressed XML files archived in the [ceneton-database](https://github.com/kws/ceneton-database) repository, so you can process the archived data without needing to decompress it first.

### Available Commands

#### 1. Dump Headers (`dump_headers`)

Extract and display field mappings from a FileMaker Pro XML export:

```bash
ct-db dump_headers path/to/ceneton_export.xml
```

This outputs a CSV with original field names and their cleaned versions:
```
original_name,clean_name
"Aantal bedr.",aantal_bedr
"Auteur",auteur
...
```

#### 2. Print Stream (`print`)

Display summary information about the XML export and progress through records:

```bash
ct-db print path/to/ceneton_export.xml
```

This shows:
```
Parsing 12543 records from Ceneton
100%|████████████| 12543/12543 [00:02<00:00, 5234.12it/s]
Done
```

#### 3. Convert to JSON Lines (`to_json`)

Convert the FileMaker Pro XML export to JSONL (JSON Lines) format for easier processing:

```bash
ct-db to_json path/to/ceneton_export.xml > ceneton_data.jsonl
```

Each line in the output file will be a JSON object representing one record from the database.

#### 4. Create Database (`create_db`)

Import the XML data into a SQL database (SQLite, PostgreSQL, etc.):

```bash
# Create SQLite database
ct-db create_db path/to/ceneton_export.xml "sqlite:///ceneton.db"

# Create PostgreSQL database
ct-db create_db path/to/ceneton_export.xml "postgresql+psycopg2://user:pass@host:5432/ceneton"

# With custom field mappings
ct-db create_db path/to/ceneton_export.xml "sqlite:///ceneton.db" --mapping-column field_mappings.csv:english_name
```

The `--mapping-column` option allows you to specify custom field name mappings from a CSV file. The format can be:
- `filename.csv` (uses column index 1)
- `filename.csv:column_name` (uses named column)

#### Working with Compressed Files

All commands support both regular XML files and bzip2-compressed files (`.bz2` extension). The toolkit automatically detects and handles compression.

### Python API

You can also use the toolkit programmatically:

```python
from ceneton_database_utils.fmp_reader import read_fmp_xml
from ceneton_database_utils.sql import create_database_and_schema

# Read FMP XML
with open('ceneton_export.xml', 'rb') as f:
    stream = read_fmp_xml(f)
    schema = next(stream)  # Get schema information
    
    # Process records
    for record in stream:
        print(record.auteur, record.titel)

# Create database programmatically
engine, metadata, table = create_database_and_schema(
    "sqlite:///ceneton.db", 
    schema
)
```

## Data Format

The original Ceneton dataset is provided in FileMaker Pro XML format. This toolkit processes that format and can output to:

- **JSONL** (JSON Lines) - One JSON object per line, suitable for streaming processing
- **SQL Database** - SQLite, PostgreSQL, or any SQLAlchemy-supported database
- **CSV** - Field mappings and headers

## License and Attribution

**Please note:** The database content is the intellectual property of Leiden University and A.J.E. Harmsen; this repository makes no claim of ownership over the data. This toolkit and all code assets are released under the [MIT License](LICENSE).

For questions about the original Ceneton database, contact: **a.j.e.harmsen@hum.leidenuniv.nl**

## Acknowledgements

The Ceneton project was led by **A.J.E. Harmsen** at Leiden University. Ceneton has been supported by the Gratama Foundation and is cited in scholarly and journalistic work on Dutch literary digitization.
