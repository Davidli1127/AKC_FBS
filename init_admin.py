#!/usr/bin/env python
"""Initialize admin users table and default admin account."""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import db
import hashlib

def _hash_password(password):
    """Hash password using SHA256"""
    return hashlib.sha256(password.encode()).hexdigest()

def init_admin_users_table():
    """Create AdminUsers table and default admin account if not exists"""
    conn = db.get_fbs_connection()
    if not conn:
        print("❌ Cannot connect to AKC_FBS database")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Create table if not exists
        sql_create = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'AdminUsers' AND xtype = 'U')
        BEGIN
            CREATE TABLE AdminUsers (
                admin_id         UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
                username         NVARCHAR(100)    NOT NULL UNIQUE,
                password_hash    NVARCHAR(255)    NOT NULL,
                email            NVARCHAR(200)    NULL,
                created_at       DATETIME         NOT NULL DEFAULT GETDATE(),
                is_active        BIT              NOT NULL DEFAULT 1
            );
            PRINT 'AdminUsers table created successfully';
        END
        ELSE
        BEGIN
            PRINT 'AdminUsers table already exists';
        END
        """
        
        cursor.execute(sql_create)
        
        # Check if default admin exists
        cursor.execute("SELECT COUNT(*) FROM AdminUsers WHERE username = 'admin'")
        admin_exists = cursor.fetchone()[0] > 0
        
        if not admin_exists:
            # Insert default admin account (password: akc2026)
            default_password_hash = _hash_password('akc2026')
            cursor.execute(
                "INSERT INTO AdminUsers (username, password_hash, email, is_active) VALUES (?, ?, ?, 1)",
                ('admin', default_password_hash, 'admin@akc.local')
            )
            conn.commit()
            print("✅ Default admin account created:")
            print("   Username: admin")
            print("   Password: akc2026")
        else:
            print("✅ Admin account already exists")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == '__main__':
    print("Initializing AdminUsers table...")
    if init_admin_users_table():
        print("✅ Initialization complete!")
    else:
        print("❌ Initialization failed!")
        sys.exit(1)
