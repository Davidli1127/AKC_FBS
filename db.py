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


def get_course_dates():
    """
    Fetch all distinct Registration Dates from the Course Participant table.
    Returns a list of date strings (YYYY-MM-DD).
    """
    conn = get_connection()
    if not conn:
        return []

    try:
        cursor = conn.cursor()
        query = f"""
            SELECT DISTINCT CAST([Registration Date] AS DATE)
            FROM {PARTICIPANT_TABLE}
            WHERE [Registration Date] IS NOT NULL
            ORDER BY CAST([Registration Date] AS DATE) DESC
        """
        cursor.execute(query)
        dates = []
        for row in cursor.fetchall():
            if row[0]:
                dates.append(str(row[0]))
        conn.close()
        return dates
    except Exception as e:
        print(f"Error fetching course dates: {e}")
        return []


def get_class_codes_by_date(registration_date):
    """
    Fetch all distinct Class Codes for a specific Registration Date.
    Returns a list of class code strings.
    """
    conn = get_connection()
    if not conn:
        return []

    try:
        cursor = conn.cursor()
        query = f"""
            SELECT DISTINCT [Class Code]
            FROM {PARTICIPANT_TABLE}
            WHERE CAST([Registration Date] AS DATE) = ?
            AND [Class Code] IS NOT NULL AND [Class Code] != ''
            ORDER BY [Class Code]
        """
        cursor.execute(query, (registration_date,))
        codes = []
        for row in cursor.fetchall():
            if row[0]:
                codes.append(str(row[0]).strip())
        conn.close()
        return codes
    except Exception as e:
        print(f"Error fetching class codes by date: {e}")
        return []


def verify_student_participant(class_code, participant_name, ic_number):
    """
    Verify if a student is a registered participant of the given class.
    - Name matching is case-insensitive and whitespace-normalized.
    - IC number matching is case-insensitive and trimmed.
    Returns True if found, False otherwise.
    """
    conn = get_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()
        query = f"""
            SELECT [Participant Name], [Identification Number]
            FROM {PARTICIPANT_TABLE}
            WHERE [Class Code] = ?
        """
        cursor.execute(query, (class_code,))
        rows = cursor.fetchall()
        conn.close()

        # Normalize input: collapse whitespace, uppercase
        name_input = ' '.join(participant_name.strip().split()).upper()
        ic_input = ic_number.strip().upper()

        for row in rows:
            db_name = ' '.join((row[0] or '').strip().split()).upper()
            db_ic = (row[1] or '').strip().upper()
            if db_name == name_input and db_ic == ic_input:
                return True

        return False
    except Exception as e:
        print(f"Error verifying student participant: {e}")
        return False


def get_participants_by_class(class_code, offset=0, limit=20):
    """
    Fetch participants for a specific class with pagination.
    Returns dict with participants list, total count, and pagination info.
    Uses [Class Code] column instead of [Course Code].
    """
    conn = get_connection()
    if not conn:
        return {'error': 'Could not connect to database'}
    
    try:
        cursor = conn.cursor()
        search_pattern = f'%{class_code}%'
        count_query = f"""
            SELECT COUNT(*) 
            FROM {PARTICIPANT_TABLE}
            WHERE [Class Code] LIKE ?
        """
        cursor.execute(count_query, (search_pattern,))
        total_count = cursor.fetchone()[0]
        query = f"""
            SELECT 
                [Class Code],
                [Participant Name],
                [Email Address],
                [Trainee Designation],
                [Survey Sent]
            FROM {PARTICIPANT_TABLE}
            WHERE [Class Code] LIKE ?
            ORDER BY [Participant Name]
            OFFSET ? ROWS
            FETCH NEXT ? ROWS ONLY
        """
        
        cursor.execute(query, (search_pattern, offset, limit))
        
        participants = []
        for row in cursor.fetchall():
            participants.append({
                'class_code': row[0] if row[0] else '',
                'name': row[1] if row[1] else '',
                'email': row[2] if row[2] else '',
                'designation': row[3] if row[3] else '',
                'survey_sent': bool(row[4]) if row[4] else False
            })
        
        conn.close()
        return {
            'participants': participants,
            'total': total_count,
            'offset': offset,
            'limit': limit,
            'has_more': (offset + limit) < total_count
        }
    except Exception as e:
        print(f"Error fetching participants: {e}")
        return {'error': str(e)}


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
    Matches the actual form structure from config.json.
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
            course_id, course_title, course_date, classroom, language,
            instructor1_name, instructor2_name, instructor3_name,
            A1, A2, A3, A4, A5,
            B1_1, B1_2, B1_3, B1_4, B1_5, B1_6,
            B2_1, B2_2, B2_3, B2_4, B2_5, B2_6,
            B3_1, B3_2, B3_3, B3_4, B3_5, B3_6,
            C1, C2, D, E,
            E1, E2, F, G, H
        ) VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?, ?
        )
        """
        
        instructor1_name = instructors[0] if len(instructors) > 0 else None
        instructor2_name = instructors[1] if len(instructors) > 1 else None
        instructor3_name = instructors[2] if len(instructors) > 2 else None
        
        values = [
            course_id,
            course.get('course_title', ''),
            course.get('course_date', ''),
            course.get('classroom', ''),
            None, 
            instructor1_name,
            instructor2_name,
            instructor3_name,
            data.get('A1'), data.get('A2'), data.get('A3'),
            data.get('A4'), data.get('A5'),
            data.get('B1_1'), data.get('B1_2'), data.get('B1_3'),
            data.get('B1_4'), data.get('B1_5'), data.get('B1_6'),
            data.get('B2_1'), data.get('B2_2'), data.get('B2_3'),
            data.get('B2_4'), data.get('B2_5'), data.get('B2_6'),
            data.get('B3_1'), data.get('B3_2'), data.get('B3_3'),
            data.get('B3_4'), data.get('B3_5'), data.get('B3_6'),
            data.get('C1'), data.get('C2'),
            data.get('D'), data.get('E'),
            data.get('E1'), data.get('E2'), data.get('F'),
            data.get('G'), data.get('H')
        ]
        
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
    Matches the actual form structure from config.json.
    """
    conn = get_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        assessors = course.get('assessors', [])
        
        query = f"""
        INSERT INTO {FEEDBACK_FORM2_TABLE} (
            course_id, course_title, course_date, classroom, language,
            assessor1_name, assessor2_name,
            A1_1, A1_2, A1_3, A1_4, A1_5,
            A2_1, A2_2, A2_3, A2_4, A2_5,
            B
        ) VALUES (
            ?, ?, ?, ?, ?,
            ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?
        )
        """
        
        assessor1_name = assessors[0] if len(assessors) > 0 else None
        assessor2_name = assessors[1] if len(assessors) > 1 else None
        
        values = [
            course_id,
            course.get('course_title', ''),
            course.get('course_date', ''),
            course.get('classroom', ''),
            None,  
            assessor1_name,
            assessor2_name,
            data.get('A1_1'), data.get('A1_2'), data.get('A1_3'),
            data.get('A1_4'), data.get('A1_5'),
            data.get('A2_1'), data.get('A2_2'), data.get('A2_3'),
            data.get('A2_4'), data.get('A2_5'),
            data.get('B')
        ]
        
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
