from datetime import datetime, timedelta
import pymysql
from db.postgres_management import PostgresManagement  # Your MySQL management class
from werkzeug.security import generate_password_hash, check_password_hash
import uuid
import jwt
import os

SECRET_KEY = os.getenv("SECRET_KEY", "default_key_if_not_set")
ALGORITHM = "HS256"

# Initialize MySQL Client
mysql_client =  PostgresManagement(
        user="admin_railway",
        password="Welcome@3210",
        host="caasrailwat.postgres.database.azure.com",
        database="railwayproject",
        port=5432
    )

table_name = "users_table"

class User:
    """User Model for MySQL"""

    def __init__(self):
        pass

    def create(self, data):
        """Create a new user in MySQL"""
        verification_code = Utility.random_with_N_digits(6)
        role = "tempAdmin"
        password = generate_password_hash(data["password"])

        user_uuid = str(uuid.uuid4())  # Generate unique UUID for MySQL
        create_user = {
            "user_uuid": user_uuid,
            "first_name": data["first_name"],
            "last_name": data["last_name"],
            "username": data["username"],
            "phone": data["phone"],
            "email": data["email"],
            "is_active": False,
            "verification_code": verification_code,
            "is_verified": False,
            "role": role,
            "password": password
        }

        try:
            if mysql_client.is_table_present(table_name):
                existing_user = mysql_client.find_record(table_name, "email=%s", (data["email"],))
                if existing_user is None:
                    mysql_client.insert_record(table_name, create_user)
                    return {"status": True, "otp": verification_code}
                else:
                    return {"status": False, "message": "User already exists"}
            else:
                mysql_client.create_table(table_name, """
                    id SERIAL PRIMARY KEY,
                    user_uuid UUID UNIQUE DEFAULT gen_random_uuid(),
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    username VARCHAR(255) UNIQUE,
                    phone VARCHAR(20),
                    email VARCHAR(255) UNIQUE,
                    is_active BOOLEAN DEFAULT FALSE,
                    verification_code VARCHAR(10),
                    is_verified BOOLEAN DEFAULT FALSE,
                    role VARCHAR(50),
                    password TEXT
                """)
                mysql_client.insert_record(table_name, create_user)
                return {"status": True, "otp": verification_code}
        except Exception as e:
            raise Exception(f"Something went wrong while creating user: {str(e)}")

    def get_all(self):
        """Get all active users"""
        users = mysql_client.find_all_records(table_name, "is_active=%s", (True,))
        return users if users else []

    def get_by_id(self, user_uuid):
        """Get a user by UUID"""
        user = mysql_client.find_record(table_name, "user_uuid=%s", (user_uuid,))
        return user if user else None

    # def get_by_email(self, email):
    #     """Get a user by email"""
    #     user = mysql_client.find_record(table_name, "email=%s", (email,))
    #     return user if user and user.get("is_active") else None
    
    def get_by_email(self, email):
        """Get a user by email"""
        user = mysql_client.find_record(table_name, "email=%s", (email,))
        return user if user else None

    def update(self, user_uuid, update_data):
        """Update a user"""
        user = self.get_by_id(user_uuid)
        if user:
            mysql_client.update_record(table_name, "user_uuid=%s", update_data, (user_uuid,))
            return self.get_by_id(user_uuid)
        return None
    
    def update_signup_user(self, email, update_data):
        """
        Updates user data dynamically.
        """
        try:
            mysql_client.update_record(table_name, "email=%s", update_data, (email,))
            return mysql_client.find_record("users", "email=%s", (email,))
        except Exception as e:
            raise Exception(f"(update_signup_user): Failed updating user\n{str(e)}")

    def delete(self, user_uuid):
        """Delete a user"""
        user = self.get_by_id(user_uuid)
        if user:
            mysql_client.delete_record(table_name, "user_uuid=%s", (user_uuid,))
            return {"status": True, "message": "User deleted"}
        return {"status": False, "message": "User not found"}

    def disable_account(self, user_uuid):
        """Disable a user account"""
        user = self.get_by_id(user_uuid)
        if user:
            mysql_client.update_record(table_name, "user_uuid=%s", {"is_active": False}, (user_uuid,))
            return {"status": True, "message": "User disabled"}
        return {"status": False, "message": "User not found"}

    def encrypt_password(self, password):
        """Encrypt password"""
        return generate_password_hash(password)

    def login(self, email, password):
        """Login a user"""
        user = self.get_by_email(email)
        print(user)
        if not user or not check_password_hash(user["password"], password):
            return {"status": False, "message": "Invalid credentials"}
        
        return {"status": True, "user_uuid": user["user_uuid"], "email": user["email"]}
    
    def create_access_token(self, user_uuid: str):
        """Generate JWT token"""
        expire = datetime.utcnow() + timedelta(days=1)
        payload = {"sub": user_uuid, "exp": expire}
        return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

    

from random import randint

class Utility(object):

    def random_with_N_digits(n):
        range_start = 10**(n-1)
        range_end = (10**n)-1
        return randint(range_start, range_end)