"""
Enterprise-grade Authentication System with JWT and RBAC
"""

import jwt
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from enum import Enum
from dataclasses import dataclass
import bcrypt
import secrets
import logging

from fastapi import HTTPException, status, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from passlib.context import CryptContext
from jose import JWTError
import asyncpg

from src.database.connection import get_async_db
from config import JWT_SECRET_KEY

logger = logging.getLogger(__name__)

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# JWT Settings
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
REFRESH_TOKEN_EXPIRE_DAYS = 7

# Security
security = HTTPBearer()

class UserRole(Enum):
    """User roles with hierarchical permissions"""
    SUPER_ADMIN = "super_admin"      # Full system access
    ADMIN = "admin"                  # Database admin, user management
    DATA_ENGINEER = "data_engineer"  # ETL, schema changes, optimization
    DATA_ANALYST = "data_analyst"    # Query execution, dashboard access
    VIEWER = "viewer"                # Read-only access to dashboards
    API_USER = "api_user"           # Programmatic access

@dataclass
class User:
    user_id: str
    username: str
    email: str
    role: UserRole
    is_active: bool
    created_at: datetime
    last_login: Optional[datetime] = None
    permissions: List[str] = None

@dataclass
class TokenData:
    user_id: str
    username: str
    role: str
    exp: datetime

class AuthenticationService:
    """
    Enterprise authentication service with advanced security features
    """
    
    def __init__(self):
        self.secret_key = JWT_SECRET_KEY
        if self.secret_key == "dev-secret-key-change-in-production":
            logger.warning("🚨 Using default JWT secret! Change in production!")
    
    def create_password_hash(self, password: str) -> str:
        """Create secure password hash"""
        return pwd_context.hash(password)
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify password against hash"""
        return pwd_context.verify(plain_password, hashed_password)
    
    def create_access_token(self, data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
        """Create JWT access token"""
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        
        to_encode.update({"exp": expire, "type": "access"})
        return jwt.encode(to_encode, self.secret_key, algorithm=ALGORITHM)
    
    def create_refresh_token(self, user_id: str) -> str:
        """Create JWT refresh token"""
        expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
        to_encode = {"sub": user_id, "exp": expire, "type": "refresh"}
        return jwt.encode(to_encode, self.secret_key, algorithm=ALGORITHM)
    
    def verify_token(self, token: str) -> TokenData:
        """Verify and decode JWT token"""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[ALGORITHM])
            user_id: str = payload.get("sub")
            username: str = payload.get("username")
            role: str = payload.get("role")
            exp: datetime = datetime.fromtimestamp(payload.get("exp"))
            
            if user_id is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid token: missing user ID"
                )
            
            return TokenData(user_id=user_id, username=username, role=role, exp=exp)
            
        except JWTError as e:
            logger.error(f"JWT verification failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token"
            )
    
    async def authenticate_user(self, username: str, password: str) -> Optional[User]:
        """Authenticate user with username/password"""
        async with get_async_db() as conn:
            # Get user from database
            user_record = await conn.fetchrow("""
                SELECT user_id, username, email, password_hash, role, is_active, 
                       created_at, last_login
                FROM auth_users 
                WHERE username = $1 OR email = $1
            """, username)
            
            if not user_record:
                logger.warning(f"Authentication failed: user not found - {username}")
                return None
            
            if not self.verify_password(password, user_record['password_hash']):
                logger.warning(f"Authentication failed: invalid password - {username}")
                return None
            
            if not user_record['is_active']:
                logger.warning(f"Authentication failed: inactive user - {username}")
                return None
            
            # Update last login
            await conn.execute("""
                UPDATE auth_users 
                SET last_login = CURRENT_TIMESTAMP 
                WHERE user_id = $1
            """, user_record['user_id'])
            
            # Get user permissions
            permissions = await conn.fetch("""
                SELECT permission_name 
                FROM auth_role_permissions rp
                JOIN auth_permissions p ON rp.permission_id = p.permission_id
                WHERE rp.role = $1
            """, user_record['role'])
            
            return User(
                user_id=user_record['user_id'],
                username=user_record['username'],
                email=user_record['email'],
                role=UserRole(user_record['role']),
                is_active=user_record['is_active'],
                created_at=user_record['created_at'],
                last_login=user_record['last_login'],
                permissions=[p['permission_name'] for p in permissions]
            )
    
    async def create_user(self, username: str, email: str, password: str, 
                         role: UserRole = UserRole.VIEWER) -> User:
        """Create new user account"""
        async with get_async_db() as conn:
            # Check if user already exists
            existing = await conn.fetchval("""
                SELECT user_id FROM auth_users 
                WHERE username = $1 OR email = $2
            """, username, email)
            
            if existing:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Username or email already registered"
                )
            
            # Create user
            user_id = secrets.token_urlsafe(16)
            password_hash = self.create_password_hash(password)
            
            await conn.execute("""
                INSERT INTO auth_users (user_id, username, email, password_hash, role, is_active)
                VALUES ($1, $2, $3, $4, $5, true)
            """, user_id, username, email, password_hash, role.value)
            
            logger.info(f"Created new user: {username} ({role.value})")
            
            return User(
                user_id=user_id,
                username=username,
                email=email,
                role=role,
                is_active=True,
                created_at=datetime.utcnow()
            )

class RoleBasedAccessControl:
    """
    Advanced Role-Based Access Control (RBAC) system
    """
    
    # Define role hierarchy (higher roles inherit lower role permissions)
    ROLE_HIERARCHY = {
        UserRole.SUPER_ADMIN: 1000,
        UserRole.ADMIN: 800,
        UserRole.DATA_ENGINEER: 600,
        UserRole.DATA_ANALYST: 400,
        UserRole.VIEWER: 200,
        UserRole.API_USER: 100
    }
    
    # Permission definitions
    PERMISSIONS = {
        # System administration
        "system:admin": [UserRole.SUPER_ADMIN],
        "users:manage": [UserRole.SUPER_ADMIN, UserRole.ADMIN],
        
        # Database operations
        "database:read": [UserRole.SUPER_ADMIN, UserRole.ADMIN, UserRole.DATA_ENGINEER, UserRole.DATA_ANALYST, UserRole.VIEWER],
        "database:write": [UserRole.SUPER_ADMIN, UserRole.ADMIN, UserRole.DATA_ENGINEER],
        "database:schema": [UserRole.SUPER_ADMIN, UserRole.ADMIN, UserRole.DATA_ENGINEER],
        
        # AI and optimization
        "ai:optimize": [UserRole.SUPER_ADMIN, UserRole.ADMIN, UserRole.DATA_ENGINEER],
        "ai:analyze": [UserRole.SUPER_ADMIN, UserRole.ADMIN, UserRole.DATA_ENGINEER, UserRole.DATA_ANALYST],
        
        # ETL and pipelines
        "etl:execute": [UserRole.SUPER_ADMIN, UserRole.ADMIN, UserRole.DATA_ENGINEER],
        "etl:monitor": [UserRole.SUPER_ADMIN, UserRole.ADMIN, UserRole.DATA_ENGINEER, UserRole.DATA_ANALYST],
        
        # Dashboards and reporting
        "dashboard:view": [UserRole.SUPER_ADMIN, UserRole.ADMIN, UserRole.DATA_ENGINEER, UserRole.DATA_ANALYST, UserRole.VIEWER],
        "dashboard:create": [UserRole.SUPER_ADMIN, UserRole.ADMIN, UserRole.DATA_ENGINEER, UserRole.DATA_ANALYST],
        
        # API access
        "api:read": [UserRole.SUPER_ADMIN, UserRole.ADMIN, UserRole.DATA_ENGINEER, UserRole.DATA_ANALYST, UserRole.API_USER],
        "api:write": [UserRole.SUPER_ADMIN, UserRole.ADMIN, UserRole.DATA_ENGINEER, UserRole.API_USER],
    }
    
    @classmethod
    def check_permission(cls, user_role: UserRole, permission: str) -> bool:
        """Check if user role has specific permission"""
        allowed_roles = cls.PERMISSIONS.get(permission, [])
        return user_role in allowed_roles
    
    @classmethod
    def check_role_hierarchy(cls, user_role: UserRole, required_role: UserRole) -> bool:
        """Check if user role meets minimum role requirement"""
        user_level = cls.ROLE_HIERARCHY.get(user_role, 0)
        required_level = cls.ROLE_HIERARCHY.get(required_role, 0)
        return user_level >= required_level

# Authentication dependency
auth_service = AuthenticationService()
rbac = RoleBasedAccessControl()

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> User:
    """FastAPI dependency to get current authenticated user"""
    token = credentials.credentials
    token_data = auth_service.verify_token(token)
    
    async with get_async_db() as conn:
        user_record = await conn.fetchrow("""
            SELECT user_id, username, email, role, is_active, created_at, last_login
            FROM auth_users 
            WHERE user_id = $1
        """, token_data.user_id)
        
        if not user_record or not user_record['is_active']:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found or inactive"
            )
        
        return User(
            user_id=user_record['user_id'],
            username=user_record['username'],
            email=user_record['email'],
            role=UserRole(user_record['role']),
            is_active=user_record['is_active'],
            created_at=user_record['created_at'],
            last_login=user_record['last_login']
        )

def require_permission(permission: str):
    """Decorator to require specific permission"""
    def permission_checker(current_user: User = Depends(get_current_user)) -> User:
        if not rbac.check_permission(current_user.role, permission):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Insufficient permissions: {permission} required"
            )
        return current_user
    return permission_checker

def require_role(min_role: UserRole):
    """Decorator to require minimum role level"""
    def role_checker(current_user: User = Depends(get_current_user)) -> User:
        if not rbac.check_role_hierarchy(current_user.role, min_role):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Insufficient role: {min_role.value} required"
            )
        return current_user
    return role_checker
