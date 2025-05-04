import pandas as pd
# from db.postgres_management import PostgresManagement
import uuid
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

    def delete_record_all(self, table_name):
        """Deletes multiple records from the table that match the condition."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"DELETE FROM {table_name};")
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

def main():
    # Instantiate the PostgreSQL management class with your connection details.
    postgres_manager = PostgresManagement(
        user="admin_railway",
        password="Welcome@3210",
        host="caasrailwat.postgres.database.azure.com",
        database="railwayproject",
        port=5432
    )

    # # 1. get_connection and close_connection
    # print("=== get_connection / close_connection ===")
    # conn = postgres_manager.get_connection()
    # print("Connection acquired!")
    # postgres_manager.release_connection(conn)
    # print("Connection released.\n")

    # # 2. is_table_present: Check if 'users' table exists.
    # print("=== is_table_present ===")
    # table_exists = postgres_manager.is_table_present("users")
    # print(f"Table 'users' exists? {table_exists}\n")

    # # 3. create_table: Create 'users' table.
    # print("=== create_table ===")
    # schema = "user_uuid UUID PRIMARY KEY, first_name TEXT, last_name TEXT, email TEXT UNIQUE"
    # table_created = postgres_manager.create_table("users", schema)
    # print(f"Table 'users' created? {table_created}\n")

    # # 4. insert_record: Insert a single user.
    # print("=== insert_record ===")
    # user_data = {
    #     "user_uuid": str(uuid.uuid4()),
    #     "first_name": "John",
    #     "last_name": "Doe",
    #     "email": "john.doe@example.com"
    # }
    # inserted_user = postgres_manager.insert_record("users", user_data)
    # print(f"Inserted User: {inserted_user}\n")

    # 5. find_record: Retrieve a user by email.
    print("=== find_record ===")
    found_user = postgres_manager.find_record("users_table", "email=%s", ("test@gmail.com",))
    print(found_user)
    print(f"""Found User: {found_user if found_user else None}\n""")

    # # 6. find_all_records: Retrieve all users.
    # print("=== find_all_records ===")
    # all_users = postgres_manager.find_all_records("users_table")
    # print(f"All Users: {all_users}\n")

    # # 7. update_record: Update a user's first name.
    # print("=== update_record ===")
    # updated_user = postgres_manager.update_record("users", "email=%s", {"first_name": "Johnny"}, ("john.doe@example.com",))
    # print(f"Updated User: {updated_user}\n")

    # # 8. update_one_record2: Update using variant method.
    # print("=== update_one_record2 ===")
    # updated_user2 = postgres_manager.update_one_record2("users", "email=%s", {"last_name": "Dane"}, ("john.doe@example.com",))
    # print(f"Updated User (Variant): {updated_user2}\n")

    # # 9. update_multiple_records: Update multiple users.
    # print("=== update_multiple_records ===")
    # multiple_updates = postgres_manager.update_multiple_records("users", "first_name=%s", {"last_name": "Smith"}, ("Johnny",))
    # print(f"Rows Updated: {multiple_updates}\n")

    # # 10. delete_record: Delete a user by email.
    # print("=== delete_record ===")
    # delete_status = postgres_manager.delete_record("users", "email=%s", ("john.doe@example.com",))
    # print(f"Delete Status: {delete_status}\n")

    # # 11. delete_records: Delete multiple users based on condition.
    # print("=== delete_records ===")
    # multiple_deletions = postgres_manager.delete_records("users_table", "last_name=%s", ("Smith",))
    # print(f"Deleted Rows: {multiple_deletions}\n")

    # # 1111. delete_records: Delete multiple users based on condition.
    # print("=== delete_records ===")
    # multiple_deletions = postgres_manager.delete_record_all("users")
    # print(f"Deleted Rows: {multiple_deletions}\n")
    

    # # 12. get_dataframe_of_collection: Retrieve users as a Pandas DataFrame.
    # print("=== get_dataframe_of_collection ===")
    # df = postgres_manager.get_dataframe_of_collection("users")
    # print("DataFrame of 'users':")
    # print(df, "\n")

    # # 13. save_dataframe_into_collection: Save DataFrame into the database.
    # print("=== save_dataframe_into_collection ===")
    # new_data = {"user_uuid": [str(uuid.uuid4()), str(uuid.uuid4())], "first_name": ["Alice", "Bob"], "last_name": ["Smith", "Johnson"], "email": ["alice@example.com", "bob@example.com"]}
    # df_to_save = pd.DataFrame(new_data)
    # save_status = postgres_manager.save_dataframe_into_collection("users", df_to_save)
    # print(f"Save DataFrame status: {save_status}\n")

    # # 14. get_result_to_display_on_browser: Retrieve data in dictionary format for web display.
    # print("=== get_result_to_display_on_browser ===")
    # browser_data = postgres_manager.get_result_to_display_on_browser("users")
    # print("Result for browser display:")
    # print(browser_data)

if __name__ == "__main__":
    main()
    # cnx = psycopg2.connect(user="admin_railway", password="Welcome@3210", host="caasrailwat.postgres.database.azure.com", port=5432, database="railwayproject")
    # cnx.cursor()