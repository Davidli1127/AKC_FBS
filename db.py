"""
Database module for SQL Server connection and operations.
Handles course lookups, participant queries, and feedback storage.
"""

import os
import pyodbc
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_SERVER = os.getenv('DB_SERVER', '10.64.2.18')
DB_DATABASE = os.getenv('DB_DATABASE', 'AKC_NAV')
DB_USERNAME = os.getenv('DB_USERNAME', 'moodleLMSAdmin')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

COURSE_TABLE = '[Absolute Kinetics Consultancy$Course]'
PARTICIPANT_TABLE = '[Absolute Kinetics Consultancy$Course Participant]'
FEEDBACK_FORM1_TABLE = 'Feedback_Form1'
FEEDBACK_FORM2_TABLE = 'Feedback_Form2'


def get_connection():
    """Create and return a database connection."""
    try:
        conn_str = (
            f'DRIVER={{ODBC Driver 18 for SQL Server}};'
            f'SERVER={DB_SERVER};'
            f'DATABASE={DB_DATABASE};'
            f'UID={DB_USERNAME};'
            f'PWD={DB_PASSWORD};'
            f'TrustServerCertificate=yes'
        )
        return pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        print(f"Database connection error: {e}")
        return None


def test_connection():
    """Test if the database connection is working."""
    conn = get_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            conn.close()
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)
    return False, "Could not establish connection"


def get_courses_from_db(search_term=None, limit=50):
    """
    Fetch courses from the database for dropdown selection.
    Returns list of dicts with Code and Description.
    """
    conn = get_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        query = f"""
            SELECT TOP {limit} 
                [Code], 
                [Description]
            FROM {COURSE_TABLE}
            WHERE 1=1
        """
        
        if search_term:
            query += f" AND ([Code] LIKE ? OR [Description] LIKE ?)"
            search_pattern = f'%{search_term}%'
            cursor.execute(query + " ORDER BY [Code]", (search_pattern, search_pattern))
        else:
            cursor.execute(query + " ORDER BY [Code]")
        
        courses = []
        for row in cursor.fetchall():
            courses.append({
                'code': row[0] if row[0] else '',
                'description': row[1] if row[1] else ''
            })
        
        conn.close()
        return courses
    except Exception as e:
        print(f"Error fetching courses: {e}")
        return []


def get_participants_by_course(course_code):
    """
    Fetch participants for a specific course.
    Returns list of participant info (name, email, designation, survey_sent).
    """
    conn = get_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        
        query = f"""
            SELECT 
                [Course Code],
                [Participant Name],
                [Email Address],
                [Trainee Designation],
                [Survey Sent]
            FROM {PARTICIPANT_TABLE}
            WHERE [Course Code] = ?
            ORDER BY [Participant Name]
        """
        
        cursor.execute(query, (course_code,))
        
        participants = []
        for row in cursor.fetchall():
            participants.append({
                'course_code': row[0] if row[0] else '',
                'name': row[1] if row[1] else '',
                'email': row[2] if row[2] else '',
                'designation': row[3] if row[3] else '',
                'survey_sent': row[4] if row[4] else False
            })
        
        conn.close()
        return participants
    except Exception as e:
        print(f"Error fetching participants: {e}")
        return []


def update_survey_sent(course_code, participant_name, sent=True):
    """
    Update the Survey Sent flag for a participant.
    """
    conn = get_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        query = f"""
            UPDATE {PARTICIPANT_TABLE}
            SET [Survey Sent] = ?
            WHERE [Course Code] = ? AND [Participant Name] = ?
        """
        
        cursor.execute(query, (1 if sent else 0, course_code, participant_name))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error updating survey sent: {e}")
        return False


def create_feedback_tables():
    """
    Create the feedback tables in the database if they don't exist.
    """
    conn = get_connection()
    if not conn:
        return False, "Could not connect to database"
    
    try:
        cursor = conn.cursor()
        form1_create = f"""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{FEEDBACK_FORM1_TABLE}' AND xtype='U')
        CREATE TABLE {FEEDBACK_FORM1_TABLE} (
            id INT IDENTITY(1,1) PRIMARY KEY,
            submission_time DATETIME DEFAULT GETDATE(),
            course_id NVARCHAR(50),
            course_title NVARCHAR(500),
            course_date NVARCHAR(50),
            classroom NVARCHAR(200),
            participant_name NVARCHAR(200),
            position NVARCHAR(200),
            -- Section A: Course Evaluation
            A1 INT, A2 INT, A3 INT, A4 INT, A5 INT, A6 INT, A7 INT, A8 INT, A9 INT,
            -- Section B: Instructor 1
            instructor1_name NVARCHAR(200),
            B1_Q1 INT, B1_Q2 INT, B1_Q3 INT, B1_Q4 INT, B1_Q5 INT, B1_Q6 INT,
            -- Section B: Instructor 2
            instructor2_name NVARCHAR(200),
            B2_Q1 INT, B2_Q2 INT, B2_Q3 INT, B2_Q4 INT, B2_Q5 INT, B2_Q6 INT,
            -- Section B: Instructor 3
            instructor3_name NVARCHAR(200),
            B3_Q1 INT, B3_Q2 INT, B3_Q3 INT, B3_Q4 INT, B3_Q5 INT, B3_Q6 INT,
            -- Section C: Text feedback
            C1 NVARCHAR(MAX),
            C2 NVARCHAR(MAX)
        )
        """
        cursor.execute(form1_create)
        form2_create = f"""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{FEEDBACK_FORM2_TABLE}' AND xtype='U')
        CREATE TABLE {FEEDBACK_FORM2_TABLE} (
            id INT IDENTITY(1,1) PRIMARY KEY,
            submission_time DATETIME DEFAULT GETDATE(),
            course_id NVARCHAR(50),
            course_title NVARCHAR(500),
            course_date NVARCHAR(50),
            assessment_location NVARCHAR(200),
            participant_name NVARCHAR(200),
            position NVARCHAR(200),
            -- Section A: Assessment Evaluation
            A1 INT, A2 INT, A3 INT, A4 INT, A5 INT, A6 INT, A7 INT,
            -- Section B: Assessor 1
            assessor1_name NVARCHAR(200),
            A1_Q1 INT, A1_Q2 INT, A1_Q3 INT, A1_Q4 INT, A1_Q5 INT, A1_Q6 INT,
            -- Section B: Assessor 2
            assessor2_name NVARCHAR(200),
            A2_Q1 INT, A2_Q2 INT, A2_Q3 INT, A2_Q4 INT, A2_Q5 INT, A2_Q6 INT,
            -- Section C: Text feedback
            C1 NVARCHAR(MAX),
            C2 NVARCHAR(MAX)
        )
        """
        cursor.execute(form2_create)
        
        conn.commit()
        conn.close()
        return True, "Tables created successfully"
    except Exception as e:
        return False, f"Error creating tables: {e}"


def save_form1_to_db(course_id, course, data):
    """
    Save Form 1 (Trainer Evaluation) response to database.
    """
    conn = get_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        instructors = course.get('instructors', [])
        num_instructors = course.get('num_instructors', 1)
        
        query = f"""
        INSERT INTO {FEEDBACK_FORM1_TABLE} (
            course_id, course_title, course_date, classroom,
            participant_name, position,
            A1, A2, A3, A4, A5, A6, A7, A8, A9,
            instructor1_name, B1_Q1, B1_Q2, B1_Q3, B1_Q4, B1_Q5, B1_Q6,
            instructor2_name, B2_Q1, B2_Q2, B2_Q3, B2_Q4, B2_Q5, B2_Q6,
            instructor3_name, B3_Q1, B3_Q2, B3_Q3, B3_Q4, B3_Q5, B3_Q6,
            C1, C2
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        values = [
            course_id,
            course.get('course_title', ''),
            course.get('course_date', ''),
            course.get('classroom', ''),
            data.get('name', ''),
            data.get('position', ''),
            data.get('A1'), data.get('A2'), data.get('A3'),
            data.get('A4'), data.get('A5'), data.get('A6'),
            data.get('A7'), data.get('A8'), data.get('A9'),
        ]
        
        if num_instructors >= 1 and len(instructors) >= 1:
            values.extend([
                instructors[0],
                data.get('B1_Q1'), data.get('B1_Q2'), data.get('B1_Q3'),
                data.get('B1_Q4'), data.get('B1_Q5'), data.get('B1_Q6')
            ])
        else:
            values.extend([None, None, None, None, None, None, None])
        
        if num_instructors >= 2 and len(instructors) >= 2:
            values.extend([
                instructors[1],
                data.get('B2_Q1'), data.get('B2_Q2'), data.get('B2_Q3'),
                data.get('B2_Q4'), data.get('B2_Q5'), data.get('B2_Q6')
            ])
        else:
            values.extend([None, None, None, None, None, None, None])
        
        if num_instructors >= 3 and len(instructors) >= 3:
            values.extend([
                instructors[2],
                data.get('B3_Q1'), data.get('B3_Q2'), data.get('B3_Q3'),
                data.get('B3_Q4'), data.get('B3_Q5'), data.get('B3_Q6')
            ])
        else:
            values.extend([None, None, None, None, None, None, None])
        
        values.extend([
            data.get('C1', ''),
            data.get('C2', '')
        ])
        
        cursor.execute(query, values)
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error saving Form 1 to database: {e}")
        return False


def save_form2_to_db(course_id, course, data):
    """
    Save Form 2 (Assessor Evaluation) response to database.
    """
    conn = get_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        assessors = course.get('assessors', [])
        num_assessors = course.get('num_assessors', 1)
        
        query = f"""
        INSERT INTO {FEEDBACK_FORM2_TABLE} (
            course_id, course_title, course_date, assessment_location,
            participant_name, position,
            A1, A2, A3, A4, A5, A6, A7,
            assessor1_name, A1_Q1, A1_Q2, A1_Q3, A1_Q4, A1_Q5, A1_Q6,
            assessor2_name, A2_Q1, A2_Q2, A2_Q3, A2_Q4, A2_Q5, A2_Q6,
            C1, C2
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        values = [
            course_id,
            course.get('course_title', ''),
            course.get('course_date', ''),
            course.get('assessment_location', ''),
            data.get('name', ''),
            data.get('position', ''),
            data.get('A1'), data.get('A2'), data.get('A3'),
            data.get('A4'), data.get('A5'), data.get('A6'), data.get('A7'),
        ]
        
        if num_assessors >= 1 and len(assessors) >= 1:
            values.extend([
                assessors[0],
                data.get('A1_Q1'), data.get('A1_Q2'), data.get('A1_Q3'),
                data.get('A1_Q4'), data.get('A1_Q5'), data.get('A1_Q6')
            ])
        else:
            values.extend([None, None, None, None, None, None, None])
        
        if num_assessors >= 2 and len(assessors) >= 2:
            values.extend([
                assessors[1],
                data.get('A2_Q1'), data.get('A2_Q2'), data.get('A2_Q3'),
                data.get('A2_Q4'), data.get('A2_Q5'), data.get('A2_Q6')
            ])
        else:
            values.extend([None, None, None, None, None, None, None])
        
        values.extend([
            data.get('C1', ''),
            data.get('C2', '')
        ])
        
        cursor.execute(query, values)
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error saving Form 2 to database: {e}")
        return False


def save_to_database(form_id, course_id, course, data):
    """
    Save form response to the appropriate database table.
    """
    if form_id == 'form1':
        return save_form1_to_db(course_id, course, data)
    elif form_id == 'form2':
        return save_form2_to_db(course_id, course, data)
    return False
