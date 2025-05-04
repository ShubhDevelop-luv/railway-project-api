from typing import Optional
from uuid import UUID
from pydantic import BaseModel, EmailStr, Field

class User(BaseModel):
    user_uuid: UUID
    username: str
    password: str = Field(..., min_length=6)
    phone: Optional[str]
    email: EmailStr
    first_name: str
    last_name: str

class Signup(BaseModel):
    password: str = Field(..., min_length=6)
    phone: Optional[str]
    email: EmailStr
    first_name: str
    last_name: str

class Login(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=6)

class ForgetPassword(BaseModel):
    email: EmailStr

class ForgetPasswordVerified(BaseModel):
    password: str = Field(..., min_length=6)
    email: EmailStr
    otp: str