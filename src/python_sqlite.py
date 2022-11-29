from sqlite3 import Connection, connect
from enum import Enum, auto
from uuid import uuid4

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

class IDType(Enum):
    SEQUENTIAL = auto()
    UUID4 = auto()

class DB:    
    # Holds the db connection object
    conn: Connection = None

    def __init__(self, path):
        self.path = path        

    def __str__(self):
        connected = "Connected" if self.conn else "Disconnected"
        return f"Connection db path: {self.path}\nConnection Status: {connected}"

    def _last_insert_rowid(self):
        sql = "select last_insert_rowid()"
        cur = self.conn.cursor()
        cur.execute(sql)
        return str(cur.fetchone()[0])

    def open_conn(self):
        if not self.conn:
            self.conn = connect(self.path)
    
    def close_conn(self):
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def push(
        self,
        table_name: str,
        data: dict,
        id_type: IDType = IDType.SEQUENTIAL,
        id: str = None) -> str:
        """Sends data to the database
        If the table doesn't exist yet it is created, and the first row is added
        If the table already exists and there is no id passed in, add a new row to the table
        If the table already exists and there is an id passed in, update that row to the table
        Returns the id of the row that was created in the database
        """
        self.open_conn()
        if not self.table_exists(table_name):
            # Create a new table
            self.create_table(table_name, data, id_type)
            return self.insert(table_name, data, id_type)
        else:
            if id:
                return self.update(table_name, data, id)     
            else:                
                return self.insert(table_name, data, id_type)
        self.close_conn()

    def table_exists(self, table_name: str) -> bool:
        """Returns true if a table exists in the database"""
        sql =  f"select name from sqlite_master where type='table' and name='{table_name}'"        
        cur = self.conn.cursor()
        res = cur.execute(sql)
        exists = res.fetchone() is not None        
        return exists

    def create_table(
        self,
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
                case IDType.UUID4:
                    # Use a random UUID4 string key.
                    id_field = "  \"id\" text not null unique,\n"
                    pk_field = "  primary key(\"id\")\n"
           
            # Generate the sql required to create each column in the schema dict
            data_fields = ""
            for name, value in schema.items():
                data_fields += f"  \"{name}\" {_sqlite_type(value)},\n"
            
            # Generate the full sql string that will be sent to the database
            fields = id_field + data_fields + pk_field
            sql = f"create table \"{table_name}\" (\n{fields});"

            # Create the table in the database
            cur = self.conn.cursor()
            cur.execute(sql)

    def insert(
        self,
        table_name: str,
        data: dict,
        id_type: IDType = IDType.SEQUENTIAL) -> str:
        """Insert a row into the database
        Return the id of the row that was inserted
        """
        row_id = ""
        if len(data.keys()) > 0:
            # Create the insert sql statement
            columns = ""
            values = ""            
            match id_type:
                case IDType.SEQUENTIAL:
                    columns = ""
                    values = ""
                case IDType.UUID4:
                    row_id = str(uuid4())
                    columns = "id,"
                    values = f"'{row_id}',"
            for column, value in data.items():
                columns += f"{column},"
                if _sqlite_type(value) == "text":
                    values += f"'{str(value)}',"
                else:
                    values += f"{str(value)},"
            columns = columns.strip(',')
            values = values.strip(',')
            sql = f"insert into {table_name}\n  ({columns})\nvalues\n  ({values});"

            # Insert the row in the database
            cur = self.conn.cursor()
            cur.execute(sql)
            if id_type == IDType.SEQUENTIAL:
                # Retrieve the row id that was just created
                row_id = self._last_insert_rowid()
            self.conn.commit()

        return row_id

    def update(
        self,
        table_name: str,
        data: dict,
        id: str) -> str:
        """Update a row in the database
        Return the id of the row that was updated"""
        if len(data.keys()) > 0:
            fields = ""
            for column, value in data.items():
                if _sqlite_type(value) == "text":
                    fields += f"{column} = '{str(value)}',"
                else:
                    fields += f"{column} = {str(value)},"
            fields = fields.strip(',')
            sql = f"update {table_name}\nset {fields}\nwhere id ='{id}'"

            # Update the row in the database
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()

        return id


employee = {
    "first_name": "William",
    "last_name": "Billiam",
    "age": 35,
    "hourly_rate": 26.50,
    "year_to_date_hours": 40.0,
    "has_license": True,
}

npc = {
    "name": "Willy the Goblin",
    "health": 67.8,
    "gold": 999
}

# create_table("employees", employee, IDType.UUID4)
#print(table_exists("employees"))
db = DB("src/test.db")
db.open_conn()
if not db.table_exists("seq"):
    print("Created seq table")
    db.create_table("seq", npc, IDType.SEQUENTIAL)

if not db.table_exists("ran"):
    print("Created ran table")
    db.create_table("ran", npc, IDType.UUID4)

print(db.update("seq", npc, '3'))
print(db.update("ran", npc, '39117145-a0d7-4dd3-bddf-1a02b351fa2a'))

# print(db.insert("seq", npc, IDType.SEQUENTIAL))
# print(db.insert("seq", npc, IDType.SEQUENTIAL))
# print(db.insert("ran", npc, IDType.UUID4))
# print(db.insert("ran", npc, IDType.UUID4))

db.close_conn()
