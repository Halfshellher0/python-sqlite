import sqlite3
from enum import Enum, auto

class IDType(Enum):
    SEQUENTIAL = auto()
    UUID4 = auto()

def _sqlite_type(var) -> str:
    """Return the sqlite datatype for a given python variable"""
    match var:
        case bool():
            return "text"
        case int():
            return "integer"
        case str():
            return "text"
        case float():
            return "real"
        case _:
            return ""

def create_table(
    table_name: str,
    schema: dict,
    id_type: IDType = IDType.SEQUENTIAL) -> None:
    """Create a table in the database based on a specified schema and id_type.

    schema is a python dictionary of a sample entry that would inserted into a table.
        Supported schema datatypes: [str, int, float, bool]

    id_type is how the primary key is constructed for the table:
        IDType.SEQUENTIAL: Create a sequential integer id
        IDType.UUID4: Create a random UUID4 id
    """
    if len(schema.keys()) > 0:
        # Generate the sql required for the id (primary key) field based on type 
        id_field = ""
        pk_field = ""
        match id_type:
            case IDType.SEQUENTIAL:
                # Use a sequential integer key.
                id_field = "  \"id\" integer not null unique,\n"
                pk_field = "  primary key(\"id\" autoincrement)\n"
                pass
            case IDType.UUID4:
                # Use a random UUID4 string key.
                id_field = "  \"id\" text not null unique,\n"
                pk_field = "  primary key(\"id\")\n"
                pass
        
        # Generate the sql required to create each column in the schema dict
        data_fields = ""
        for name, value in schema.items():
            data_fields += f"  \"{name}\" {_sqlite_type(value)},\n"
        
        # Generate the full sql string that will be sent to the database
        fields = id_field + data_fields + pk_field
        sql = f"create table \"{table_name}\" (\n{fields});"

        # Create the table in the database
        con = sqlite3.connect("src/test.db")
        cur = con.cursor()
        cur.execute(sql)

employee = {
    "first_name": "William",
    "last_name": "Billiam",
    "age": 35,
    "hourly_rate": 26.50,
    "year_to_date_hours": 40.0,
    "has_license": True,
}

create_table("employees", employee, IDType.UUID4)
