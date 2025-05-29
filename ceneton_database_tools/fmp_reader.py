"""
A module for ingesting FileMaker Pro XML exports into a Python-friendly format
"""
from collections import namedtuple
from enum import Enum
from typing import Any, Callable, Dict, Generator, List, NamedTuple, Protocol, Type, TypeAlias
from lxml import etree as ET
from dataclasses import dataclass
from logging import getLogger
import re

logger = getLogger(__name__)

class ParserState(Enum):
    INITIAL = "initial"
    HEADER = "header"
    RESULTS = "results"

class FMPFieldType(Enum):
    TEXT = "text"
    NUMBER = "number"



@dataclass
class FMPField:
    name: str
    empty_ok: bool = False
    max_repeat: int = 1
    type: FMPFieldType = FMPFieldType.TEXT

    
@dataclass
class FMPSchema:
    name: str
    field_type: Type[NamedTuple]
    name_mapping: Dict[str, FMPField]
    record_count: int

HeaderStrategy: TypeAlias = Callable[[List[FMPField]], List[str]]

def sanitize_name(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r'\W+', "_", name)
    if re.match(r'^\d', name):
        name = "_" + name
    return name

def default_header_strategy(original_names: List[FMPField]) -> List[str]:
    new_names = []
    for original_name in original_names:
        name = sanitize_name(original_name.name)
        if name in new_names:
            raise ValueError(f"Duplicate field name: {name}")
        new_names.append(name)

    return new_names

FMP_DEFAULT_NS = "http://www.filemaker.com/fmp/1/result"
NS_LENGTH = len(FMP_DEFAULT_NS) + 2

def read_fmp_xml(source: Any, header_strategy: HeaderStrategy = default_header_strategy) -> Generator[Any, None, None]:
    """
    Read a FileMaker Pro XML export and yield a generator of dictionaries,
    one for each record in the export.
    """
    parser_state = ParserState.INITIAL
    header_data: Dict[str, str] = {}
    field_list: List[FMPField] = []
    field_type: tuple | None = None

    database_record_count: int | None = None
    resultset_record_count: int | None = None

    current_record: List[str] = []

    for event, elem in ET.iterparse(source, events=('start', 'end',)):
        tag_name = elem.tag[NS_LENGTH:]

        # We are now in the results section - we do this first here because this is the majority of the file so we reduce the number of "checks" we need to do
        if parser_state == ParserState.RESULTS:
            if tag_name == "ROW":
                if event == 'start':
                    current_record.clear()
                elif event == 'end':
                    yield field_type._make(current_record)
                    current_record.clear()

            elif tag_name == "DATA" and event == 'end':
                current_record.append(elem.text)

            # We're done with this element, so we can continue to next event
            continue

        # In header mode we handle either the <DATABASE> element giving overall info, or <FIELD> elements with field details                    
        if parser_state == ParserState.HEADER and event == 'end':

            if tag_name == "DATABASE":
                # Just stick them all in the header_data dict
                header_data.update(elem.attrib)
                database_record_count = int(header_data["RECORDS"])

            elif tag_name == "FIELD":
                #     <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="Aantal bedr." TYPE="TEXT"/>
                field_name = elem.attrib["NAME"]
                field_type = FMPFieldType[elem.attrib["TYPE"]]
                field_empty_ok = elem.attrib["EMPTYOK"] == "YES"
                field_max_repeat = int(elem.attrib["MAXREPEAT"])
                field_list.append(FMPField(name=field_name, type=field_type, empty_ok=field_empty_ok, max_repeat=field_max_repeat))

            continue
                
        
        # We are still in "header" mode, but we receive the start of the results section
        # Now it's time to coalesce the header and prepare to receive data
        if parser_state == ParserState.HEADER and event == 'start' and tag_name == "RESULTSET":
            parser_state = ParserState.RESULTS
            resultset_record_count = int(elem.attrib["FOUND"])
            if resultset_record_count != database_record_count:
                logger.warning(f"Number of records in header ({database_record_count}) does not match number of records in result set ({resultset_record_count}). We are looking at a subset of the database.")
            
            # We can now create the data enum
            cleaned_names = header_strategy(field_list)
            assert len(set(cleaned_names)) == len(field_list), f"There are duplicate 'cleaned' names in the field list: {cleaned_names}. You will have to provide your own header_strategy to avoid this."
            field_type = namedtuple("Field", cleaned_names)

            name_mapping = {n: f for n, f in zip(cleaned_names, field_list)}
            yield FMPSchema(name=header_data["NAME"], field_type=field_type, name_mapping=name_mapping, record_count=resultset_record_count)
            

        # Start of the file - we are ready to receive the header
        if parser_state == ParserState.INITIAL and event == 'start' and tag_name == "FMPXMLRESULT":
            parser_state = ParserState.HEADER
            continue


