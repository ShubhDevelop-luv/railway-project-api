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
        (Wrapper for find_all_records.)
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


# ==================== Examples for Each Function ====================

def main():
    # Instantiate the MySQL management class with your connection details.
    mysql_manager = MySQLManagement(
        host="database-1.cits8i2sy323.us-east-1.rds.amazonaws.com",
        user="admin",
        password="wLRBHnuxxhRETwExta9Y",
        database="sqldb",
        port=3306
    )

    # 1. get_connection and close_connection
    print("=== get_connection / close_connection ===")
    conn = mysql_manager.get_connection()
    print("Connection acquired!")
    mysql_manager.close_connection(conn)
    print("Connection closed.\n")

    # 2. is_database_present: Check if 'sqldb' exists.
    print("=== is_database_present ===")
    db_exists = mysql_manager.is_database_present("sqldb")
    print(f"Database 'sqldb' exists? {db_exists}\n")

    # 3. create_database: Create a test database 'testdb'
    print("=== create_database ===")
    created_db = mysql_manager.create_database("testdb")
    print(f"Database 'testdb' created? {created_db}")

    # 4. drop_database: Drop 'testdb'
    print("=== drop_database ===")
    dropped_db = mysql_manager.drop_database("testdb")
    print(f"Database 'testdb' dropped? {dropped_db}\n")

    # 5. is_table_present: Check for table 'users'
    print("=== is_table_present ===")
    table_exists = mysql_manager.is_table_present("users")
    print(f"Table 'users' exists? {table_exists}\n")

    # 6. create_table: Create table 'users'
    print("=== create_table ===")
    schema = "id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), age INT"
    created_table = mysql_manager.create_table("users", schema)
    print(f"Table 'users' created? {created_table}\n")

    # 7. drop_table: Create a temporary table then drop it.
    print("=== drop_table ===")
    temp_schema = "id INT AUTO_INCREMENT PRIMARY KEY, data VARCHAR(100)"
    mysql_manager.create_table("temp_table", temp_schema)
    dropped_table = mysql_manager.drop_table("temp_table")
    print(f"Temporary table 'temp_table' dropped? {dropped_table}\n")

    # 8. insert_record: Insert a single record into 'users'
    print("=== insert_record ===")
    record = {"name": "Alice", "age": 30}
    inserted_id = mysql_manager.insert_record("users", record)
    print(f"Inserted record ID: {inserted_id}\n")

    # 9. insert_records: Insert multiple records into 'users'
    print("=== insert_records ===")
    records = [
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35}
    ]
    multi_insert_status = mysql_manager.insert_records("users", records)
    print(f"Multiple records inserted: {multi_insert_status}\n")

    # 10. find_first_record: Get the first record where name = 'Alice'
    print("=== find_first_record ===")
    first_record = mysql_manager.find_first_record("users", "name=%s", params=("Alice",))
    print(f"First record (name='Alice'): {first_record}\n")

    # 11. find_all_records: Retrieve all records from 'users'
    print("=== find_all_records ===")
    all_records = mysql_manager.find_all_records("users")
    print(f"All records: {all_records}\n")

    # 12. find_record_on_query: Find records where age > 25
    print("=== find_record_on_query ===")
    records_query = mysql_manager.find_record_on_query("users", "age>%s", params=(25,))
    print(f"Records with age > 25: {records_query}\n")

    # 13. update_one_record: Update one record (set age=28) where name = 'Bob'
    print("=== update_one_record ===")
    rows_updated = mysql_manager.update_one_record("users", "name=%s", {"age": 28}, params=("Bob",))
    print(f"Rows updated (update_one_record): {rows_updated}\n")

    # 14. update_one_record2: Variant update, e.g., update 'Charlie' to set age=40
    print("=== update_one_record2 ===")
    rows_updated2 = mysql_manager.update_one_record2("users", "name=%s", {"age": 40}, params=("Charlie",))
    print(f"Rows updated (update_one_record2): {rows_updated2}\n")

    # 15. update_multiple_records: Update all records where age < 35, set age=35
    print("=== update_multiple_records ===")
    multiple_updated_count = mysql_manager.update_multiple_records("users", "age < %s", {"age": 35}, params=(35,))
    print(f"Rows updated (multiple): {multiple_updated_count}\n")

    # 16. delete_record: Delete one record where name = 'Alice'
    print("=== delete_record ===")
    del_status = mysql_manager.delete_record("users", "name=%s", params=("Alice",))
    print(f"Delete record status: {del_status}\n")

    # 17. delete_records: Delete records where age = 35
    print("=== delete_records ===")
    del_multiple_status = mysql_manager.delete_records("users", "age=%s", params=(35,))
    print(f"Delete records status: {del_multiple_status}\n")

    # 18. get_dataframe_of_collection: Retrieve a DataFrame of 'users'
    print("=== get_dataframe_of_collection ===")
    df = mysql_manager.get_dataframe_of_collection("users")
    print("DataFrame from 'users':")
    print(df, "\n")

    # 19. save_dataframe_into_collection: Create a DataFrame and save it into 'users'
    print("=== save_dataframe_into_collection ===")
    new_data = {"name": ["David", "Eve"], "age": [42, 29]}
    df_to_save = pd.DataFrame(new_data)
    save_status = mysql_manager.save_dataframe_into_collection("users", df_to_save)
    print(f"DataFrame save status: {save_status}\n")

    # 20. get_result_to_display_on_browser: Get a list of records for display in a browser
    print("=== get_result_to_display_on_browser ===")
    final_results = mysql_manager.get_result_to_display_on_browser("users")
    print("Final records for browser display:")
    print(final_results)

if __name__ == "__main__":
    main()