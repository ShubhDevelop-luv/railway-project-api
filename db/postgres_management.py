import psycopg2
from psycopg2 import pool
import pandas as pd
import uuid


class PostgresManagement:
    def __init__(self, user, password, host, database, port=5432):
        """
        Initializes Azure PostgreSQL connection pool using `psycopg2`.
        """
        try:
            self.pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                user=user,
                password=password,
                host=host,
                port=port,
                database=database
            )
            if not self.pool:
                raise Exception("Failed to create PostgreSQL connection pool.")

        except Exception as e:
            raise Exception(f"(__init__): Failed to initialize PostgreSQL pool\n{str(e)}")

    def get_connection(self):
        """Gets a connection from the pool."""
        return self.pool.getconn()

    def release_connection(self, conn):
        """Releases the connection back to the pool."""
        self.pool.putconn(conn)

    def close_pool(self):
        """Closes all connections in the pool."""
        self.pool.closeall()

    def is_table_present(self, table_name):
        """Checks if a table exists in the Azure PostgreSQL database."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name=%s);",
                    (table_name,)
                )
                return cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"(is_table_present): Failed checking table existence\n{str(e)}")
        finally:
            self.release_connection(conn)

    def create_table(self, table_name, schema):
        """Creates a table with the given schema."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema});")
                conn.commit()
        except Exception as e:
            raise Exception(f"(create_table): Failed to create table '{table_name}'\n{str(e)}")
        finally:
            self.release_connection(conn)

    def insert_record(self, table_name, record):
        """
        Inserts a single record into the specified table.
        Prevents duplicate entries by dynamically checking primary/unique constraints before inserting.
        """
        conn = self.get_connection()
        try:
            keys = list(record.keys())
            values = list(record.values())
            columns = ", ".join(keys)
            placeholders = ", ".join(["%s"] * len(values))

            with conn.cursor() as cursor:
                try:
                    # Attempt to insert the record directly
                    cursor.execute(
                        f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) RETURNING *;",
                        values
                    )
                    conn.commit()
                    return {"status": True, "message": "Record inserted successfully", "data": cursor.fetchone()}
                
                except psycopg2.errors.UniqueViolation:
                    conn.rollback()
                    return {"status": False, "message": "Record already exists due to unique constraint"}
                
                except psycopg2.DatabaseError as db_err:
                    conn.rollback()
                    return {"status": False, "message": f"Database error: {str(db_err)}"}
        
        except Exception as e:
            return {"status": False, "message": f"(insert_record): Unexpected error - {str(e)}"}
        
        finally:
            self.release_connection(conn)

    def find_record(self, table_name, condition, params):
        """
        Retrieves the first record that matches the condition.
        Converts the result from a tuple to a dictionary.
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table_name} WHERE {condition} LIMIT 1;", params)
                record = cursor.fetchone()

                if record:
                    # Convert tuple to dictionary using cursor.description
                    column_names = [desc[0] for desc in cursor.description]
                    return dict(zip(column_names, record))
                
                return None  # If no matching record exists
        except Exception as e:
            raise Exception(f"(find_record): Failed fetching record from '{table_name}'\n{str(e)}")
        finally:
            self.release_connection(conn)

    def find_all_records(self, table_name, condition="", params=()):
        """
        Retrieves all records matching the condition.
        Converts the result from a tuple to a list of dictionaries.
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                query = f"SELECT * FROM {table_name} "
                if condition:
                    query += f"WHERE {condition}"

                cursor.execute(query, params)
                records = cursor.fetchall()

                if records:
                    column_names = [desc[0] for desc in cursor.description]  # Get column names
                    return [dict(zip(column_names, row)) for row in records]  # Convert tuples to dicts

                return []  # Return an empty list if no records found
        except Exception as e:
            raise Exception(f"(find_all_records): Failed fetching records from '{table_name}'\n{str(e)}")
        finally:
            self.release_connection(conn)
            
    def update_record(self, table_name, condition, update_data, params):
        """Updates a record based on the condition."""
        conn = self.get_connection()
        try:
            set_clause = ", ".join([f"{key}=%s" for key in update_data.keys()])
            values = list(update_data.values())

            with conn.cursor() as cursor:
                cursor.execute(
                    f"UPDATE {table_name} SET {set_clause} WHERE {condition} RETURNING *;",
                    values + list(params)
                )
                conn.commit()
                return cursor.fetchone()
        except Exception as e:
            raise Exception(f"(update_record): Failed updating record in '{table_name}'\n{str(e)}")
        finally:
            self.release_connection(conn)

    def update_one_record2(self, table_name, condition, update, params=()):
        """Variant of update_one_record."""
        try:
            return self.update_record(table_name, condition, update, params)
        except Exception as e:
            raise Exception(f"(update_one_record2): {str(e)}")

    def update_multiple_records(self, table_name, condition, update_dict, params=()):
        """Updates multiple records in the table that meet the given condition."""
        conn = self.get_connection()
        try:
            set_clause = ", ".join([f"{key}=%s" for key in update_dict.keys()])
            values = list(update_dict.values())

            with conn.cursor() as cursor:
                cursor.execute(
                    f"UPDATE {table_name} SET {set_clause} WHERE {condition};",
                    values + list(params)
                )
                conn.commit()
                return cursor.rowcount
        except Exception as e:
            raise Exception(f"(update_multiple_records): Failed to update records in '{table_name}'\n{str(e)}")
        finally:
            self.release_connection(conn)

    def delete_record(self, table_name, condition, params=()):
        """
        Deletes a single record from the table that matches the condition.
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                sql = f"DELETE FROM {table_name} WHERE {condition} RETURNING *;"
                cursor.execute(sql, params)
                conn.commit()
                return f"{cursor.rowcount} row deleted" if cursor.rowcount > 0 else "No row deleted"
        except Exception as e:
            raise Exception(f"(delete_record): Failed to delete record from '{table_name}'\n{str(e)}")
        finally:
            self.release_connection(conn)


    def delete_records(self, table_name, condition, params=()):
        """Deletes multiple records from the table that match the condition."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"DELETE FROM {table_name} WHERE {condition};", params)
                conn.commit()
                return f"{cursor.rowcount} rows deleted"
        except Exception as e:
            raise Exception(f"(delete_records): Failed to delete records from '{table_name}'\n{str(e)}")
        finally:
            self.release_connection(conn)
    
    def get_dataframe_of_collection(self, table_name, condition="", params=()):
        """
        Returns a Pandas DataFrame of the records from the table that satisfy the condition.
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                query = f"SELECT * FROM {table_name} "
                if condition:
                    query += f"WHERE {condition}"

                cursor.execute(query, params)
                records = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                dataframe = pd.DataFrame(records, columns=columns)
                return dataframe
        except Exception as e:
            raise Exception(f"(get_dataframe_of_collection): Failed to get DataFrame from '{table_name}'\n{str(e)}")
        finally:
            self.release_connection(conn)

    def save_dataframe_into_collection(self, table_name, dataframe):
        """
        Saves a Pandas DataFrame into the table.
        Converts the DataFrame into a list of dictionaries and inserts them.
        """
        conn = self.get_connection()
        try:
            records = dataframe.to_dict(orient="records")
            keys = records[0].keys()
            columns = ", ".join(keys)
            placeholders = ", ".join(["%s"] * len(keys))

            with conn.cursor() as cursor:
                for record in records:
                    values = tuple(record[key] for key in keys)
                    cursor.execute(
                        f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) RETURNING *;",
                        values
                    )

                conn.commit()
            return "Inserted successfully"
        except Exception as e:
            raise Exception(f"(save_dataframe_into_collection): Failed to save DataFrame into '{table_name}'\n{str(e)}")
        finally:
            self.release_connection(conn)

    def get_result_to_display_on_browser(self, table_name, condition="", params=()):
        """
        Returns all records from the table (optionally matching a condition) as a list,
        for displaying on a browser.
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                query = f"SELECT * FROM {table_name} "
                if condition:
                    query += f"WHERE {condition}"

                cursor.execute(query, params)
                response = cursor.fetchall()
                result = [dict(zip([desc[0] for desc in cursor.description], row)) for row in response]
                
                return result
        except Exception as e:
            raise Exception(f"(get_result_to_display_on_browser): Something went wrong with '{table_name}'\n{str(e)}")
        finally:
            self.release_connection(conn)