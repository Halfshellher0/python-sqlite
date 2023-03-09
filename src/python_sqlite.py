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

class Database:    
    # Holds the db connection object
    conn: Connection = None

    def __init__(self, path):
        self.path = path        

    def __str__(self):
        connected = "Connected" if self.conn else "Disconnected"
        return f"Connection db path: {self.path}\nConnection Status: {connected}"

    def __enter__(self):
        self.open_conn()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close_conn()

    def _last_insert_rowid(self):
        """Retrieves the most recently created rowid in the database."""
        sql = "select last_insert_rowid()"
        cur = self.conn.cursor()
        cur.execute(sql)
        return str(cur.fetchone()[0])

    def open_conn(self):
        """Opens a connection with the database."""
        print("Opening connection")
        if not self.conn:
            self.conn = connect(self.path)
    
    def close_conn(self):
        """Closes the connection to the database."""
        print("Closing connection")
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def push(
        self,
        table_name: str,
        data: dict,
        id_type: IDType = IDType.SEQUENTIAL) -> str:
        """Pushes data to the database.

        If the table doesn't exist yet it is created, and the first row is added.
        Otherwise insert a new row in the database, unless an id is provided then
        an update to an existing row is performed.

        Args:
          table_name: 
            Name of the database table to push data to.
          data: 
            A data dictionary representing one row in the table.
            If the data dictionary contains an id then a row update is performed,
            otherwise an insert is performed.
            Supported data types: [str, int, float, bool]
          id_type: 
            Option of how the primary key is constructed for the table.
            IDType.SEQUENTIAL: Create a sequential integer id. (Default)
            IDType.UUID4: Create a random UUID4 id.

        Returns:
          An id of the row that was inserted or updated in the database.

        Raises:

        """
        if not self.table_exists(table_name):
            # Create a new table
            self.create_table(table_name, data, id_type)
            return self.insert(table_name, data, id_type)
        else:
            # Determine the id of the row to be updated
            id = ""
            for column, value in data.items():
                if column == "id":
                    id = str(value)

            if id:
                return self.update(table_name, data)     
            else:                
                return self.insert(table_name, data, id_type)

    def table_exists(self, table_name: str) -> bool:
        """Returns true if a table exists in the database."""
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

        Takes the schema dict and converts it into a create table sql command
        using the dict keys as the column names, and the sqlite datatypes
        are converted from python types within the schema dict.

        Args:
          table_name: 
            Name of the database table to push data to.
          schema:
            A python dictionary of a sample entry that would inserted into a table.
            Supported schema datatypes: [str, int, float, bool]
          id_type: 
            Option of how the primary key is constructed for the table.
            IDType.SEQUENTIAL: Create a sequential integer id. (Default)
            IDType.UUID4: Create a random UUID4 id.

        Returns:
          None

        Raises:

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
        """Insert a row into the database.

        Insert a new row in the database table based on the data contained within
        the data dict.

        Args:
          table_name: 
            Name of the database table to insert data to.
          data: 
            A data dictionary representing one row in the table.
            Supported data types: [str, int, float, bool]
          id_type: 
            Option of how the primary key is constructed for the table.
            IDType.SEQUENTIAL: Create a sequential integer id. (Default)
            IDType.UUID4: Create a random UUID4 id.

        Returns:
          Return the id of the row that was inserted

        Raises:

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
        data: dict) -> str:
        """Update a row in the database.

        Update an existing row in the database based on the id provided.
        the updated row is updated with data contained in the data dict.

        Args:
          table_name: 
            Name of the database table to update data to.
          data: 
            A data dictionary representing one row in the table.
            The data dictionary must contain an id column.
            Supported data types: [str, int, float, bool]

        Returns:
          Return the id of the row that was updated

        Raises:

        """
        if len(data.keys()) > 0:
            
            # Determine the id of the row to be updated
            id = ""
            for column, value in data.items():
                if column == "id":
                    id = str(value)

            if not id:
                raise ValueError("There was no id found in the data dictionary, update failed")

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


    
    def select(
        self,
        table_name: str,
        id: str) -> dict:
        """
        """
        obj = {}
        
        sql = f"select *\nfrom {table_name}\nwhere id = '{id}'"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        column_headers = []
        values = []
        
        for description in cur.description:
            column_headers.append(description[0])

        for column in rows[0]:
            values.append(column)
        
        i = 0
        for header in column_headers:
            if header == "id":
                # Always make ids into strings to support compatibility between sequential and UUID4 ids
                obj[header] = str(values[i])      
            else:
                obj[header] = values[i]
            i += 1

        return obj




employee = {
    "first_name": "William",
    "last_name": "Billiam",
    "age": 35,
    "hourly_rate": 26.50,
    "year_to_date_hours": 40.0,
    "has_license": True,
}

employee2 = {
    "first_name": "Hilliam",
    "last_name": "Filliam",
    "age": 67,
    "hourly_rate": 89.70,
    "year_to_date_hours": 5.75,
    "has_license": False,
}

npc = {
    "name": "Willy the Goblin",
    "health": 67.8,
    "gold": 999
}

cards = [
    {
        "name": "Gary the Goblin"
    },
    {
        "name": "Poison pickle"
    },
    {
        "name": "Secret Door"
    },
    {
        "name": "Pete the Parrot King"
    },
    {
        "name": "BillyBot"
    },
    {
        "name": "Town Crier"
    },
    {
        "name": "Anna the Orchardsmith"
    },
    {
        "name": "Oaken Battering Club"
    },
    {
        "name": "Marmin the Strange Wizard"
    }    
]

with Database("src/test5.db") as db:
    id = db.push("npcs", npc)
    npc1 = db.select("npcs", id)
    print(npc1)
    npc1["name"] = "Helga"
    id2 = db.push("npcs", npc1)
    npc2 = db.select("npcs", id2)



#db = DB("src/readonly.db")
#for card in cards:
#    db.push("CardTemplates", card, IDType.UUID4)



#print(db.push("employees", employee2))
# print(db.push("npcs", npc, IDType.UUID4))

# with Database("src/test4.db") as db:
#     if not db.table_exists("seq"):
#         print("Created seq table")
#         db.create_table("seq", npc, IDType.SEQUENTIAL)

#     test = db.select("seq", "6")
#     test["name"] = "Helga"
#     db.push("seq", test, IDType.UUID4, "6")
#     print(test)
    # print(db.insert("seq", npc, IDType.SEQUENTIAL))
    # print(db.insert("seq", npc, IDType.SEQUENTIAL))
    # print(db.insert("seq", npc, IDType.SEQUENTIAL))
# db.open_conn()


# if not db.table_exists("ran"):
#     print("Created ran table")
#     db.create_table("ran", npc, IDType.UUID4)




# print(db.insert("seq", npc, IDType.SEQUENTIAL))
# print(db.insert("ran", npc, IDType.UUID4))
# print(db.insert("ran", npc, IDType.UUID4))

# db.close_conn()
