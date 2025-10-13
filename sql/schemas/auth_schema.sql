-- Authentication and authorization tables for enterprise security
-- This creates a complete RBAC (Role-Based Access Control) system

-- Users table
CREATE TABLE auth_users (
    user_id VARCHAR(32) PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(20) NOT NULL DEFAULT 'viewer',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    failed_login_attempts INTEGER DEFAULT 0,
    locked_until TIMESTAMP,
    
    -- Additional security fields
    email_verified BOOLEAN DEFAULT false,
    two_factor_enabled BOOLEAN DEFAULT false,
    two_factor_secret VARCHAR(32),
    
    -- Audit fields
    created_by VARCHAR(32),
    updated_by VARCHAR(32),
    
    CONSTRAINT valid_role CHECK (role IN ('super_admin', 'admin', 'data_engineer', 'data_analyst', 'viewer', 'api_user'))
);

-- Permissions table
CREATE TABLE auth_permissions (
    permission_id SERIAL PRIMARY KEY,
    permission_name VARCHAR(100) UNIQUE NOT NULL,
    description TEXT,
    category VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Role-Permission mapping
CREATE TABLE auth_role_permissions (
    role VARCHAR(20),
    permission_id INTEGER,
    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    granted_by VARCHAR(32),
    
    PRIMARY KEY (role, permission_id),
    FOREIGN KEY (permission_id) REFERENCES auth_permissions(permission_id),
    CONSTRAINT valid_role CHECK (role IN ('super_admin', 'admin', 'data_engineer', 'data_analyst', 'viewer', 'api_user'))
);

-- User sessions for tracking active sessions
CREATE TABLE auth_sessions (
    session_id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(32) NOT NULL,
    access_token_hash VARCHAR(255),
    refresh_token_hash VARCHAR(255),
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ip_address INET,
    user_agent TEXT,
    is_active BOOLEAN DEFAULT true,
    
    FOREIGN KEY (user_id) REFERENCES auth_users(user_id) ON DELETE CASCADE
);

-- API keys for programmatic access
CREATE TABLE auth_api_keys (
    api_key_id VARCHAR(32) PRIMARY KEY,
    user_id VARCHAR(32) NOT NULL,
    key_name VARCHAR(100) NOT NULL,
    key_hash VARCHAR(255) NOT NULL,
    permissions JSONB,
    is_active BOOLEAN DEFAULT true,
    expires_at TIMESTAMP,
    last_used_at TIMESTAMP,
    usage_count INTEGER DEFAULT 0,
    rate_limit_per_minute INTEGER DEFAULT 100,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (user_id) REFERENCES auth_users(user_id) ON DELETE CASCADE
);

-- Audit log for security events
CREATE TABLE auth_audit_log (
    audit_id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(32),
    action VARCHAR(50) NOT NULL,
    resource VARCHAR(100),
    details JSONB,
    ip_address INET,
    user_agent TEXT,
    success BOOLEAN NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (user_id) REFERENCES auth_users(user_id) ON DELETE SET NULL
);

-- Insert default permissions
INSERT INTO auth_permissions (permission_name, description, category) VALUES
-- System administration
('system:admin', 'Full system administration access', 'system'),
('users:manage', 'Create, update, and delete user accounts', 'system'),
('users:view', 'View user accounts and permissions', 'system'),

-- Database operations
('database:read', 'Read access to database content', 'database'),
('database:write', 'Write access to database content', 'database'), 
('database:schema', 'Modify database schema and structure', 'database'),
('database:backup', 'Create and restore database backups', 'database'),

-- AI and optimization
('ai:optimize', 'Execute AI-powered optimizations', 'ai'),
('ai:analyze', 'Run AI analysis and recommendations', 'ai'),
('ai:configure', 'Configure AI models and parameters', 'ai'),

-- ETL and pipelines
('etl:execute', 'Execute ETL pipelines and data processing', 'etl'),
('etl:monitor', 'Monitor pipeline status and performance', 'etl'),
('etl:configure', 'Configure and modify ETL pipelines', 'etl'),

-- Dashboards and reporting
('dashboard:view', 'View dashboards and reports', 'dashboard'),
('dashboard:create', 'Create and modify dashboards', 'dashboard'),
('dashboard:admin', 'Administer dashboard system', 'dashboard'),

-- API access
('api:read', 'Read-only API access', 'api'),
('api:write', 'Read-write API access', 'api'),
('api:admin', 'Administrative API access', 'api');

-- Assign permissions to roles
INSERT INTO auth_role_permissions (role, permission_id) 
SELECT 'super_admin', permission_id FROM auth_permissions; -- Super admin gets all permissions

INSERT INTO auth_role_permissions (role, permission_id) 
SELECT 'admin', permission_id FROM auth_permissions 
WHERE permission_name NOT LIKE 'system:admin'; -- Admin gets most permissions except system admin

INSERT INTO auth_role_permissions (role, permission_id)
SELECT 'data_engineer', permission_id FROM auth_permissions
WHERE category IN ('database', 'ai', 'etl', 'dashboard', 'api')
AND permission_name NOT LIKE '%:admin';

INSERT INTO auth_role_permissions (role, permission_id)
SELECT 'data_analyst', permission_id FROM auth_permissions
WHERE permission_name IN (
    'database:read', 'ai:analyze', 'etl:monitor', 
    'dashboard:view', 'dashboard:create', 'api:read'
);

INSERT INTO auth_role_permissions (role, permission_id)
SELECT 'viewer', permission_id FROM auth_permissions
WHERE permission_name IN ('database:read', 'dashboard:view');

INSERT INTO auth_role_permissions (role, permission_id)
SELECT 'api_user', permission_id FROM auth_permissions
WHERE permission_name IN ('api:read', 'api:write', 'database:read', 'database:write');

-- Create indexes for performance
CREATE INDEX idx_auth_users_username ON auth_users(username);
CREATE INDEX idx_auth_users_email ON auth_users(email);
CREATE INDEX idx_auth_users_role ON auth_users(role);
CREATE INDEX idx_auth_users_active ON auth_users(is_active);
CREATE INDEX idx_auth_sessions_user ON auth_sessions(user_id);
CREATE INDEX idx_auth_sessions_active ON auth_sessions(is_active, expires_at);
CREATE INDEX idx_auth_api_keys_user ON auth_api_keys(user_id);
CREATE INDEX idx_auth_audit_user ON auth_audit_log(user_id);
CREATE INDEX idx_auth_audit_timestamp ON auth_audit_log(timestamp);

-- Create default super admin user (password: admin123 - CHANGE IN PRODUCTION!)
INSERT INTO auth_users (user_id, username, email, password_hash, role, is_active, email_verified) 
VALUES (
    'admin_001', 
    'admin', 
    'admin@autosql.com',
    '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8.un.9H7FH8G0aU9ymu', -- bcrypt hash of 'admin123'
    'super_admin', 
    true, 
    true
);

-- Create sample data engineer user (password: engineer123)
INSERT INTO auth_users (user_id, username, email, password_hash, role, is_active, email_verified)
VALUES (
    'engineer_001',
    'data_engineer', 
    'engineer@autosql.com',
    '$2b$12$8Hw9LH7QHuKN3.0b1XgylOK8W8y9aL5H7JE3c.V8B4Q2H5m9D7KG.',
    'data_engineer',
    true,
    true
);

-- Create sample analyst user (password: analyst123) 
INSERT INTO auth_users (user_id, username, email, password_hash, role, is_active, email_verified)
VALUES (
    'analyst_001',
    'data_analyst',
    'analyst@autosql.com', 
    '$2b$12$9Iv1MJ8RJvLO4.1c2YhzmPL9X9z0bM6I8KF4d.W9C5R3I6n0E8LH.',
    'data_analyst',
    true,
    true
);

-- Functions for security
CREATE OR REPLACE FUNCTION update_last_activity()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to automatically update timestamps
CREATE TRIGGER auth_users_update_trigger
    BEFORE UPDATE ON auth_users
    FOR EACH ROW
    EXECUTE FUNCTION update_last_activity();

-- Function to log authentication events
CREATE OR REPLACE FUNCTION log_auth_event(
    p_user_id VARCHAR(32),
    p_action VARCHAR(50),
    p_resource VARCHAR(100) DEFAULT NULL,
    p_details JSONB DEFAULT NULL,
    p_ip_address INET DEFAULT NULL,
    p_user_agent TEXT DEFAULT NULL,
    p_success BOOLEAN DEFAULT true
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO auth_audit_log (user_id, action, resource, details, ip_address, user_agent, success)
    VALUES (p_user_id, p_action, p_resource, p_details, p_ip_address, p_user_agent, p_success);
END;
$$ LANGUAGE plpgsql;
