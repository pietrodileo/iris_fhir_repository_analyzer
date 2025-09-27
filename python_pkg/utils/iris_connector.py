import iris
import pandas as pd

class IRIS_connection:
    def __init__(self, host = "127.0.0.1", port = 1972, namespace = 'USER', username = '_SYSTEM', password = 'SYS'):
        """
        Initialize an IRIS connection object with the given parameters.

        Parameters:
            host (str): The hostname or IP address of the IRIS server. Defaults to "127.0.0.1".
            port (int): The port number of the IRIS superserver. Defaults to 1972.
            namespace (str): The namespace of the IRIS database. Defaults to 'USER'.
            username (str): The username to use when connecting to IRIS. Defaults to '_SYSTEM'.
            password (str): The password to use when connecting to IRIS. Defaults to 'SYS'.
        """
        self.host = host  # localhost = 127.0.0.1 
        self.port = port # superserver port
        self.namespace = namespace
        self.username = username
        self.password = password
        
        # Open a connection to the server
        args = {
            'hostname':host,
            'port': port,
            'namespace':namespace,  
            'username': username, 
            'password': password
        }
        self.conn = iris.connect(**args)

    def create_index(self, index_name: str, table_name: str, column_name: str, index_type: str = "index") -> None:
        """
        Create an index on a given table and column.

        Args:
            index_name (str): The name of the index.
            table_name (str): The table to add the index to.
            column_name (str): The column to index.
            index_type (str): Type of index (e.g., 'INDEX', 'COLUMNAR', 'BITSLICE', 'BITMAP', ''). Default: 'index'.
        """
        if self.index_exists(table_name, index_name):
            raise ValueError(f"Index {index_name} already exists on {table_name}.")
        
        if index_type != "" and index_type != "index":
            sql = f"CREATE {index_type} INDEX {index_name} ON {table_name}({column_name})"
        else:
            sql = f"CREATE INDEX {index_name} ON {table_name}({column_name})"

        cursor = self.conn.cursor()
        try:
            cursor.execute(sql)
            self.conn.commit()
            print(f"Index {index_name} created successfully on {table_name}({column_name}).")
        except Exception as e:
            self.conn.rollback()
            print(f"Error creating index {index_name} on {table_name}: {e}")
            raise
        finally:
            cursor.close()
    
    def quick_create_index(self, table_name: str, column_name: str) -> None:
        """
        Convenience function to quickly create an index on a given table and column.
        If the index already exists, it will not be re-created and a message will be printed.
        The index name will be the column name followed by "_idx".
        The index type will be standard index.
        
        Args:
            table_name (str): The table to add the index to.
            column_name (str): The column to index.

        Returns:
            None
        """
        index_name = f"{column_name}_idx"
        if not self.index_exists(table_name=table_name, index_name=index_name):
            self.create_index(
                index_name=index_name, 
                table_name=table_name, 
                column_name=column_name
            )
        else: 
            print(f"Index {index_name} already exists")
    
    def query(self, sql: str, parameters: list = []) -> pd.DataFrame:
        """
        Execute a SQL query and return the full result as a pandas DataFrame.
        Example: df = conn.query("SELECT * FROM my_table")
        """
        cursor = self.conn.cursor()
        try:         
            # execute the query   
            cursor.execute(sql,parameters)

            # Fetch all rows at once
            rows = cursor.fetchall()
            if not rows:
                return pd.DataFrame()

            # Extract column names from cursor description
            columns = [col[0] for col in cursor.description]

            # Create DataFrame in one shot
            df = pd.DataFrame(rows, columns=columns)
            return df    
        except Exception as e:
            print(f"Error executing query: {e}")
            raise   # re-raise to let caller handle it
        finally:
            cursor.close()
    
    def get_row_id(self, table_name: str, column_name: str, value: str) -> int:
        """
        Retrieve the row ID from a table given a column name and value.
        
        Args:
            table_name (str): The name of the table to query.
            column_name (str): The name of the column to search for.
            value (str): The value to search for in the column.
        
        Returns:
            int: The row ID associated with the given column name and value.
        
        Raises:
            Exception: If there is an error executing the query.
        """
        sql = f"SELECT ID FROM {table_name} WHERE {column_name} = ?"
        try:
            id_df = self.query(sql, parameters=[value])
            return int(id_df["ID"][0])
        except Exception as e:
            print(f"Error getting row ID: {e}")
            raise

    def create_table(self, table_name: str, columns: dict[str, str], constraints: list[str] | None = None, table_schema: str = None, check_exists: bool = False) -> None:
        """
        Create a table in the database.

        Args:
            table_name (str): The name of the table to create.
            columns (dict[str, str]): A dictionary of column names to their respective data types.
            constraints (list[str] | None, optional): A list of constraints to apply to the table. Defaults to None.
            table_schema (str, optional): The schema of the table to create. Defaults to None.
            check_exists (bool, optional): If True, raise an error if the table already exists. 
            
        Raises:
            ValueError: If the table already exists and check_exists is True.
            Exception: If there is an error creating the table.
        """
        if check_exists and self.table_exists(table_name=table_name, table_schema=table_schema):
            raise ValueError(f"Table {table_name} already exists.")

        if table_schema:
            table_name = f"{table_schema}.{table_name}" 
            
        col_defs = [f"{col} {ctype}" for col, ctype in columns.items()]
        if constraints:
            col_defs.extend(constraints)

        sql = f"CREATE TABLE {table_name} ( {', '.join(col_defs)} )"

        cursor = self.conn.cursor()
        try:
            # execute the query   
            cursor.execute(sql)
            self.conn.commit()
            print(f"Table {table_name} created successfully.")
        except Exception as e:
            self.conn.rollback()
            print(f"Error creating table {table_name}: {e}")
            raise

    def drop_table(self, table_name: str, if_exists: bool = True) -> None:
        """
        Drop a SQL table with the given name.
        
        Args:
            table_name (str): The name of the table to drop.
            if_exists (bool): If True, use IF EXISTS to avoid errors 
                            when the table doesn't exist (default: True).
        """
        sql = f"DROP TABLE {'IF EXISTS ' if if_exists else ''}{table_name}"

        cursor = self.conn.cursor()
        try:
            cursor.execute(sql)
            self.conn.commit()
            print(f"Table {table_name} dropped successfully.")
        except Exception as e:
            self.conn.rollback()
            print(f"Error dropping table {table_name}: {e}")
            raise
        finally:
            cursor.close()

    def describe_table(self, table_name: str) -> dict:
        """
        Retrieve metadata about a table: columns and indices.
        """
        cursor = self.conn.cursor()
        info = {}

        try:
            # Columns
            cursor.execute(f"""
                SELECT TABLE_SCHEMA, TABLE_NAME, column_name, data_type, character_maximum_length, is_nullable, AUTO_INCREMENT, UNIQUE_COLUMN, PRIMARY_KEY, odbctype
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_name = '{table_name}'
            """)
            columns = cursor.fetchall()
            info["columns"] = [
                dict(zip([col[0] for col in cursor.description], row)) for row in columns
            ]

            # Indexes
            cursor.execute(f"""
                SELECT index_name, column_name, PRIMARY_KEY, NON_UNIQUE
                FROM INFORMATION_SCHEMA.INDEXES
                WHERE table_name = '{table_name}'
            """)
            indexes = cursor.fetchall()
            info["indexes"] = [
                dict(zip([col[0] for col in cursor.description], row)) for row in indexes
            ]

        finally:
            cursor.close()

        return info

    def table_exists(self, table_name: str, table_schema: str | None = None) -> bool:
        """
        Check if a table exists in the database.

        Args:
            table_name (str): The name of the table to check.
            table_schema (str | None, optional): The schema of the table to check. Defaults to None.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        parameters = [table_name]
        query = f"""
            SELECT COUNT(*) as num_rows
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_name = ?
        """
        if table_schema:
            query = query + """
                AND table_schema = ?
            """
            parameters.append(table_schema)
        
        exists_df = self.query(query, parameters)    
        exists = int(exists_df['num_rows'][0]) > 0
        
        return exists

    def index_exists(self, table_name: str, index_name: str) -> bool:
        """
        Check if an index exists on a given table.

        Args:
            table_name (str): The name of the table to check.
            index_name (str): The name of the index to check.

        Returns:
            bool: True if the index exists, False otherwise.

        """
        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.INDEXES
            WHERE table_name = '{table_name}'
            AND index_name = '{index_name}'
        """)
        exists = cursor.fetchone()[0] > 0
        cursor.close()
        return exists

    def create_hnsw_index(self, table_name: str, column_name: str, index_name: str, distance: str = "Cosine", M: int = None, ef_construct: int = None) -> None:
        """
        InterSystems SQL allows you to define a Hierarchical Navigable Small World (HNSW) index,
        which uses the HNSW algorithm to create a vector index.

        You can define an HNSW index using a CREATE INDEX statement.  To define an HNSW index,
        the following requirements must be met:

        * The HNSW index is defined on a VECTOR-typed field with a fixed length that is of
            type FLOAT, DOUBLE, or DECIMAL.
        * The table the index is defined on must have IDs that are bitmap-supported.
        * The table the index is defined on must use default storage.

        There are three parameters you can specify when defining an HNSW index:

        * **Distance (required):** The distance function used by the index, surrounded by quotes ('').
            Possible values are Cosine and DotProduct. This parameter is case-insensitive.
        * **M (optional):** The number of bi-directional links created for every new element during
            construction. This value should be a positive integer larger than 1; the value will fall
            between 2 and 100. Higher M values work better on datasets with high dimensionality or
            recall, while lower M values work better with low dimensionality or recall.
            The default value is 64.
        * **efConstruct (optional):** The size of the dynamic list for the nearest neighbors. This
            value should be a positive integer larger than M. Larger efConstruct values generally
            lead to better index quality but longer construction time.  There is a maximum value
            past which efConstruct does not improve the quality of the index.
            The default value is 64.
        """
        cursor = self.conn.cursor()
        params = [f"Distance='{distance}'"]
        if M:
            params.append(f"M={M}")
        if ef_construct:
            params.append(f"efConstruct={ef_construct}")
        param_str = ", ".join(params)
        
        sql = f"""
            CREATE INDEX {index_name}
            ON {table_name}({column_name})
            AS %SQL.Index.HNSW({param_str})
        """
        try:
            cursor.execute(sql)
            self.conn.commit()
            print(f"Created HNSW index {index_name} on {table_name}({column_name})")
        except Exception as e:
            print(f"Failed to create HNSW index: {e}")
        finally:
            cursor.close()

    def insert(self, table_name: str, **kwargs) -> None:
        """
        Insert a row into a table.

        Args:
            table_name (str): The table to insert into.
            **kwargs: Column=value pairs.
        """
        cursor = self.conn.cursor()
        
        # Build columns and placeholders
        columns = ', '.join(kwargs.keys())
        placeholders = ', '.join(['?'] * len(kwargs))  # parameterized query

        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        try:
            cursor.execute(sql, tuple(kwargs.values()))
            self.conn.commit()
            #print(f"Inserted row into {table_name}: {kwargs}")
        except Exception as e:
            self.conn.rollback()
            print(f"Failed to insert into {table_name}: {e}")
            raise
        finally:
            cursor.close()
