import datetime
from pydantic import BaseModel
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi import APIRouter
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from typing import Optional
from models.user import User
from db.postgres_management import PostgresManagement  # Your MySQL management class

app = APIRouter()
mysql_client = PostgresManagement(
        user="admin_railway",
        password="Welcome@3210",
        host="caasrailwat.postgres.database.azure.com",
        database="railwayproject",
        port=5432
    )
table_name = "users_table"

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")
user_model = User()

# ==============================
# Authentication Endpoints
# ==============================

@app.post("/register")
def register(user_data: dict):
    """Register user"""
    response = user_model.create(user_data)
    if response["status"]:  # User successfully registered
        return {
            "status": True,
            "message": "User registered successfully. Please verify using OTP.",
            "otp": response["otp"],  # Return OTP to the frontend
            "user_uuid": response.get("user_uuid")
        }
        
    return {"status": False, "message": response["message"]}

class OTPVerification(BaseModel):
    email: str
    otp: str

@app.post("/signup/mobile_otp_verification")
def verify_user(data: OTPVerification):
    """
    Verify user and update user data in PostgreSQL database.
    """
    try:
        user = user_model.get_by_email(data.email)
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found or not registered.")
        
        if user["is_verified"]:
            return {"status": False, "message": "Mobile number already verified."}
        
        if user["verification_code"] == data.otp:
            update_data = {
                "is_verified": True,
                "is_active": True
            }
            user_model.update_signup_user(data.email, update_data)
            return {"status": True, "message": "Mobile number verified successfully."}
        
        return {"status": False, "message": "Mobile number not verified, retry with correct OTP."}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Something went wrong: {str(e)}")
    

@app.post("/login")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """Login user and return JWT token"""
    user = user_model.login(form_data.username, form_data.password)
    if not user["status"]:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    access_token = user_model.create_access_token(user["user_uuid"])
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/user", response_model=Optional[dict])
def get_user(user_uuid: str):
    """Get user details"""
    user = user_model.get_by_id(user_uuid)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user

@app.get("/users", response_model=list[dict])
def get_all_users():
    """Get all active users"""
    return user_model.get_all()

@app.put("/user/{user_uuid}")
def update_user(user_uuid: str, update_data: dict):
    """Update user"""
    return user_model.update(user_uuid, update_data)

@app.delete("/user/{user_uuid}")
def delete_user(user_uuid: str):
    """Delete user"""
    return user_model.delete(user_uuid)