import pymysql
import pandas as pd
import json

class MySQLManagement:
    def __init__(self, host, user, password, database=None, port=3306):
        """
        Initializes the connection parameters for a MySQL database.
        Optionally, a default database name can be provided.
        """
        try:
            self.host = host
            self.user = user
            self.password = password
            self.database = database  # If not provided, will use empty string for default
            self.port = port
        except Exception as e:
            raise Exception(f"(__init__): Error during initialization: {str(e)}")

    def get_connection(self):
        """
        Creates and returns a new pymysql connection object
        with the instance's connection parameters.
        """
        try:
            conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database if self.database is not None else "",
                port=self.port,
                cursorclass=pymysql.cursors.DictCursor  # Results as dictionaries.
            )
            return conn
        except Exception as e:
            raise Exception(f"(get_connection): Could not create connection: {str(e)}")

    def close_connection(self, conn):
        """
        Closes the provided connection.
        """
        try:
            conn.close()
        except Exception as e:
            raise Exception(f"(close_connection): Error closing connection: {str(e)}")

    def is_database_present(self, db_name):
        """
        Checks if the specified database exists.
        """
        try:
            conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port
            )
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES;")
            databases = [row[0] for row in cursor.fetchall()]
            self.close_connection(conn)
            return db_name in databases
        except Exception as e:
            raise Exception(f"(is_database_present): {str(e)}")

    def create_database(self, db_name):
        """
        Creates a database if it does not already exist.
        """
        try:
            conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port
            )
            cursor = conn.cursor()
            if not self.is_database_present(db_name):
                cursor.execute(f"CREATE DATABASE {db_name}")
                conn.commit()
            self.close_connection(conn)
            return True
        except Exception as e:
            raise Exception(f"(create_database): {str(e)}")

    def drop_database(self, db_name):
        """
        Drops the specified database if it exists.
        """
        try:
            conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port
            )
            cursor = conn.cursor()
            if self.is_database_present(db_name):
                cursor.execute(f"DROP DATABASE {db_name}")
                conn.commit()
            self.close_connection(conn)
            return True
        except Exception as e:
            raise Exception(f"(drop_database): {str(e)}")

    def is_table_present(self, table_name):
        """
        Checks if a table exists in the default database (set during initialization).
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES;")
            tables = [list(row.values())[0] for row in cursor.fetchall()]
            self.close_connection(conn)
            return table_name in tables
        except Exception as e:
            raise Exception(f"(is_table_present): {str(e)}")

    def create_table(self, table_name, schema):
        """
        Creates a table with the provided schema.
        Example schema: "id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), age INT"
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema});"
            cursor.execute(sql)
            conn.commit()
            self.close_connection(conn)
            return True
        except Exception as e:
            raise Exception(f"(create_table): {str(e)}")

    def drop_table(self, table_name):
        """
        Drops the specified table if it exists.
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            sql = f"DROP TABLE IF EXISTS {table_name};"
            cursor.execute(sql)
            conn.commit()
            self.close_connection(conn)
            return True
        except Exception as e:
            raise Exception(f"(drop_table): {str(e)}")

    def insert_record(self, table_name, record: dict):
        """
        Inserts a single record (as a dictionary) into the specified table.
        Returns the last inserted ID.
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            keys = record.keys()
            columns = ", ".join(keys)
            placeholders = ", ".join(["%s"] * len(keys))
            values = tuple(record[k] for k in keys)
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"
            cursor.execute(sql, values)
            conn.commit()
            last_id = cursor.lastrowid
            self.close_connection(conn)
            return last_id
        except Exception as e:
            raise Exception(f"(insert_record): {str(e)}")

    def insert_records(self, table_name, records: list):
        """
        Inserts multiple records (a list of dictionaries) into the specified table.
        Returns a status message indicating the number of rows inserted.
        """
        try:
            if not records:
                return "No records to insert"
            conn = self.get_connection()
            cursor = conn.cursor()
            keys = records[0].keys()
            columns = ", ".join(keys)
            placeholders = ", ".join(["%s"] * len(keys))
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"
            row_count = 0
            for record in records:
                values = tuple(record[k] for k in keys)
                cursor.execute(sql, values)
                row_count += cursor.rowcount
            conn.commit()
            self.close_connection(conn)
            return f"{row_count} rows inserted"
        except Exception as e:
            raise Exception(f"(insert_records): {str(e)}")

    def find_first_record(self, table_name, condition="", params=()):
        """
        Retrieves the first record from the table that satisfies the optional condition.
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            sql = f"SELECT * FROM {table_name} "
            if condition:
                sql += f"WHERE {condition} "
            sql += "LIMIT 1;"
            cursor.execute(sql, params)
            record = cursor.fetchone()
            self.close_connection(conn)
            return record
        except Exception as e:
            raise Exception(f"(find_first_record): {str(e)}")

    def find_all_records(self, table_name, condition="", params=()):
        """
        Retrieves all records from the table that satisfy the optional condition.
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            sql = f"SELECT * FROM {table_name} "
            if condition:
                sql += f"WHERE {condition}"
            cursor.execute(sql, params)
            records = cursor.fetchall()
            self.close_connection(conn)
            return records
        except Exception as e:
            raise Exception(f"(find_all_records): {str(e)}")

    # ----- Additional Methods (Converted from MongoDB Functions) -----

    def find_record_on_query(self, table_name, condition, params=()):
        """
        Finds records in the table based on the provided condition.
        """
        try:
            return self.find_all_records(table_name, condition, params)
        except Exception as e:
            raise Exception(f"(find_record_on_query): Failed to find records for given query on table '{table_name}'\n{str(e)}")

    def update_one_record(self, table_name, condition, update_dict, params=()):
        """
        Updates a single record in the table that meets the given condition.
        'update_dict' is a dictionary of column-value pairs to update.
        Only one record is updated with LIMIT 1.
        """
        try:
            set_clause = ", ".join([f"{key}=%s" for key in update_dict.keys()])
            values = list(update_dict.values())
            sql = f"UPDATE {table_name} SET {set_clause} WHERE {condition} LIMIT 1;"
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, tuple(values) + params)
            conn.commit()
            count = cursor.rowcount
            self.close_connection(conn)
            return count
        except Exception as e:
            raise Exception(f"(update_one_record): Failed to update a record in table '{table_name}'\n{str(e)}")

    def update_one_record2(self, table_name, condition, update, params=()):
        """
        Variant of update_one_record.
        'update' is a dictionary containing the columns and new values.
        """
        try:
            return self.update_one_record(table_name, condition, update, params)
        except Exception as e:
            raise Exception(f"(update_one_record2): {str(e)}")

    def update_multiple_records(self, table_name, condition, update_dict, params=()):
        """
        Updates multiple records in the table that meet the given condition.
        'update_dict' is a dictionary of column-value pairs to update.
        """
        try:
            set_clause = ", ".join([f"{key}=%s" for key in update_dict.keys()])
            values = list(update_dict.values())
            sql = f"UPDATE {table_name} SET {set_clause} WHERE {condition};"
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, tuple(values) + params)
            conn.commit()
            count = cursor.rowcount
            self.close_connection(conn)
            return count
        except Exception as e:
            raise Exception(f"(update_multiple_records): Failed to update records in table '{table_name}'\n{str(e)}")

    def delete_record(self, table_name, condition, params=()):
        """
        Deletes a single record from the table that matches the condition.
        """
        try:
            sql = f"DELETE FROM {table_name} WHERE {condition} LIMIT 1;"
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, params)
            conn.commit()
            count = cursor.rowcount
            self.close_connection(conn)
            return f"{count} row deleted" if count > 0 else "No row deleted"
        except Exception as e:
            raise Exception(f"(delete_record): Failed to delete record from table '{table_name}'\n{str(e)}")

    def delete_records(self, table_name, condition, params=()):
        """
        Deletes multiple records from the table that match the condition.
        """
        try:
            sql = f"DELETE FROM {table_name} WHERE {condition};"
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, params)
            conn.commit()
            count = cursor.rowcount
            self.close_connection(conn)
            return f"{count} rows deleted"
        except Exception as e:
            raise Exception(f"(delete_records): Failed to delete records from table '{table_name}'\n{str(e)}")

    def get_dataframe_of_collection(self, table_name, condition="", params=()):
        """
        Returns a Pandas DataFrame of the records from the table that satisfy the condition.
        """
        try:
            records = self.find_all_records(table_name, condition, params)
            dataframe = pd.DataFrame(records)
            return dataframe
        except Exception as e:
            raise Exception(f"(get_dataframe_of_collection): Failed to get DataFrame from table '{table_name}'\n{str(e)}")

    def save_dataframe_into_collection(self, table_name, dataframe):
        """
        Saves a Pandas DataFrame into the table.
        Converts the DataFrame into a list of dictionaries and inserts them.
        """
        try:
            records = dataframe.to_dict(orient="records")
            status = self.insert_records(table_name, records)
            return "Inserted" if "rows inserted" in status else status
        except Exception as e:
            raise Exception(f"(save_dataframe_into_collection): Failed to save DataFrame into table '{table_name}'\n{str(e)}")

    def get_result_to_display_on_browser(self, table_name, condition="", params=()):
        """
        Returns all records from the table (optionally matching a condition) as a list,
        for displaying on a browser.
        """
        try:
            response = self.find_all_records(table_name, condition, params)
            result = [row for row in response]
            return result
        except Exception as e:
            raise Exception(f"(get_result_to_display_on_browser): Something went wrong with table '{table_name}'\n{str(e)}")