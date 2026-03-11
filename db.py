"""
Database module for SQL Server connection and operations.

AKC_NAV  -- course catalogue and participant lookup (read-mostly).
AKC_FBS  -- feedback form registry (FBS_Forms) and responses (FBS_Responses).
"""

import os
import json
import pyodbc
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# -- Connection settings -------------------------------------------------------
DB_SERVER   = os.getenv('DB_SERVER',   '10.64.2.18')
DB_USERNAME = os.getenv('DB_USERNAME', 'moodleLMSAdmin')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

# AKC_NAV -- participant / course data
DB_NAV_DATABASE  = os.getenv('DB_DATABASE',     'AKC_NAV')
COURSE_TABLE      = '[Absolute Kinetics Consultancy$Course]'
PARTICIPANT_TABLE = '[Absolute Kinetics Consultancy$Course Participant]'

# AKC_FBS -- feedback storage
DB_FBS_DATABASE = os.getenv('DB_FBS_DATABASE', 'AKC_FBS')

_CONN_TMPL = (
    'DRIVER={{ODBC Driver 18 for SQL Server}};'
    'SERVER={server};DATABASE={db};UID={uid};PWD={pwd};TrustServerCertificate=yes'
)


def get_connection():
    """Return a connection to AKC_NAV."""
    try:
        return pyodbc.connect(_CONN_TMPL.format(
            server=DB_SERVER, db=DB_NAV_DATABASE,
            uid=DB_USERNAME, pwd=DB_PASSWORD))
    except pyodbc.Error as e:
        print(f"AKC_NAV connection error: {e}")
        return None


def get_fbs_connection():
    """Return a connection to AKC_FBS."""
    try:
        return pyodbc.connect(_CONN_TMPL.format(
            server=DB_SERVER, db=DB_FBS_DATABASE,
            uid=DB_USERNAME, pwd=DB_PASSWORD))
    except pyodbc.Error as e:
        print(f"AKC_FBS connection error: {e}")
        return None


def test_connection():
    """Test both database connections. Returns (ok: bool, message: str)."""
    results = {}
    for label, fn in [('AKC_NAV', get_connection), ('AKC_FBS', get_fbs_connection)]:
        conn = fn()
        if conn:
            try:
                conn.cursor().execute("SELECT 1")
                conn.close()
                results[label] = 'OK'
            except Exception as e:
                results[label] = str(e)
        else:
            results[label] = 'Could not connect'
    ok = all(v == 'OK' for v in results.values())
    return ok, ' | '.join(f'{k}: {v}' for k, v in results.items())


# -- AKC_FBS table initialisation ---------------------------------------------

def init_fbs_tables():
    """Create FBS_Forms and FBS_Responses tables if they do not exist."""
    conn = get_fbs_connection()
    if not conn:
        return False, "Could not connect to AKC_FBS"
    try:
        cur = conn.cursor()
        cur.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='FBS_Forms' AND xtype='U')
            CREATE TABLE FBS_Forms (
                form_id      NVARCHAR(100) NOT NULL,
                form_title   NVARCHAR(300) NOT NULL,
                form_number  NVARCHAR(100) NULL,
                description  NVARCHAR(MAX) NULL,
                config_json  NVARCHAR(MAX) NULL,
                created_at   DATETIME NOT NULL DEFAULT GETDATE(),
                updated_at   DATETIME NOT NULL DEFAULT GETDATE(),
                is_deleted   BIT NOT NULL DEFAULT 0,
                deleted_at   DATETIME NULL,
                CONSTRAINT PK_FBS_Forms PRIMARY KEY (form_id)
            )
        """)
        cur.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='FBS_Responses' AND xtype='U')
            CREATE TABLE FBS_Responses (
                response_id      INT IDENTITY(1,1) NOT NULL,
                form_id          NVARCHAR(100) NOT NULL,
                course_id        NVARCHAR(100) NOT NULL,
                class_code       NVARCHAR(100) NULL,
                course_title     NVARCHAR(500) NULL,
                course_date      NVARCHAR(50)  NULL,
                venue            NVARCHAR(200) NULL,
                submitted_at     DATETIME NOT NULL DEFAULT GETDATE(),
                participant_name NVARCHAR(200) NULL,
                id_number        NVARCHAR(100) NULL,
                position_title   NVARCHAR(200) NULL,
                instructor1_name NVARCHAR(200) NULL,
                instructor2_name NVARCHAR(200) NULL,
                instructor3_name NVARCHAR(200) NULL,
                assessor1_name   NVARCHAR(200) NULL,
                assessor2_name   NVARCHAR(200) NULL,
                answers_json     NVARCHAR(MAX) NULL,
                CONSTRAINT PK_FBS_Responses PRIMARY KEY (response_id)
            )
        """)
        conn.commit()
        conn.close()
        return True, "Tables ready"
    except Exception as e:
        return False, f"Error initialising tables: {e}"


# -- FBS_Forms registry -------------------------------------------------------

def register_form(form_id, form_title, form_number, description, config_dict):
    """
    Upsert a form in FBS_Forms.
    If the form was previously soft-deleted, it is restored (is_deleted -> 0).
    """
    conn = get_fbs_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cfg = json.dumps(config_dict, ensure_ascii=False)
        now = datetime.now()
        cur.execute("SELECT form_id FROM FBS_Forms WHERE form_id = ?", (form_id,))
        if cur.fetchone():
            cur.execute("""
                UPDATE FBS_Forms
                SET form_title = ?, form_number = ?, description = ?,
                    config_json = ?, updated_at = ?, is_deleted = 0, deleted_at = NULL
                WHERE form_id = ?
            """, (form_title, form_number or '', description or '', cfg, now, form_id))
        else:
            cur.execute("""
                INSERT INTO FBS_Forms
                    (form_id, form_title, form_number, description, config_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (form_id, form_title, form_number or '', description or '', cfg, now, now))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error registering form '{form_id}': {e}")
        return False


def soft_delete_form(form_id):
    """Mark a form as deleted. Responses in FBS_Responses are never touched."""
    conn = get_fbs_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE FBS_Forms SET is_deleted = 1, deleted_at = ? WHERE form_id = ?",
            (datetime.now(), form_id))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error soft-deleting form '{form_id}': {e}")
        return False


def find_form_by_title(form_title):
    """
    Look up a form in FBS_Forms by title (case-insensitive).
    Returns dict with form_id, form_title, is_deleted or None.
    """
    conn = get_fbs_connection()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT form_id, form_title, is_deleted FROM FBS_Forms "
            "WHERE UPPER(LTRIM(RTRIM(form_title))) = UPPER(LTRIM(RTRIM(?)))",
            (form_title,))
        row = cur.fetchone()
        conn.close()
        if row:
            return {'form_id': row[0], 'form_title': row[1], 'is_deleted': bool(row[2])}
        return None
    except Exception as e:
        print(f"Error finding form by title: {e}")
        return None


def form_has_responses(form_id):
    """Return True if FBS_Responses has at least one row for this form_id."""
    conn = get_fbs_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM FBS_Responses WHERE form_id = ?", (form_id,))
        row = cur.fetchone()
        conn.close()
        return bool(row and row[0] > 0)
    except Exception as e:
        print(f"Error checking form responses: {e}")
        return False


# -- Response storage ---------------------------------------------------------

def has_submitted_db(course_id, id_number):
    """Return True if a response already exists for (course_id, id_number)."""
    conn = get_fbs_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM FBS_Responses "
            "WHERE course_id = ? AND UPPER(LTRIM(RTRIM(id_number))) = ?",
            (course_id, id_number.strip().upper()))
        row = cur.fetchone()
        conn.close()
        return bool(row and row[0] > 0)
    except Exception as e:
        print(f"Error checking submission: {e}")
        return False


def get_submitted_ids_for_courses(course_ids):
    """
    Return a set of normalised id_numbers that have already submitted
    for any of the given course_ids.
    """
    if not course_ids:
        return set()
    conn = get_fbs_connection()
    if not conn:
        return set()
    try:
        cur = conn.cursor()
        ph  = ','.join('?' for _ in course_ids)
        cur.execute(
            f"SELECT UPPER(LTRIM(RTRIM(id_number))) FROM FBS_Responses "
            f"WHERE course_id IN ({ph})",
            list(course_ids))
        ids = {r[0] for r in cur.fetchall() if r[0]}
        conn.close()
        return ids
    except Exception as e:
        print(f"Error fetching submitted IDs: {e}")
        return set()


def save_response_to_db(form_id, course_id, course, participant_name, id_number, position, data):
    """
    Insert one response row into FBS_Responses.
    course  : dict from config courses -- provides course_title, course_date,
              classroom / assessment_location, instructors, assessors.
    data    : dict of all submitted answers {question_id: value}.
    """
    conn = get_fbs_connection()
    if not conn:
        return False
    try:
        cur         = conn.cursor()
        instructors = course.get('instructors', [])
        assessors   = course.get('assessors',   [])
        venue       = (course.get('classroom') or
                       course.get('assessment_location') or
                       course.get('venue') or '')
        cur.execute("""
            INSERT INTO FBS_Responses (
                form_id, course_id, class_code, course_title, course_date,
                venue, submitted_at, participant_name, id_number, position_title,
                instructor1_name, instructor2_name, instructor3_name,
                assessor1_name, assessor2_name, answers_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            form_id,
            course_id,
            course.get('course_title', ''),
            course.get('course_title', ''),
            course.get('course_date', ''),
            venue,
            datetime.now(),
            participant_name or '',
            (id_number or '').strip().upper(),
            position or '',
            instructors[0] if len(instructors) > 0 else None,
            instructors[1] if len(instructors) > 1 else None,
            instructors[2] if len(instructors) > 2 else None,
            assessors[0]   if len(assessors)   > 0 else None,
            assessors[1]   if len(assessors)   > 1 else None,
            json.dumps(data, ensure_ascii=False),
        ))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error saving response to DB: {e}")
        return False


# -- Analysis queries ---------------------------------------------------------

def get_response_count_by_form():
    """Return {form_id: count} for all forms with at least one response."""
    conn = get_fbs_connection()
    if not conn:
        return {}
    try:
        cur = conn.cursor()
        cur.execute("SELECT form_id, COUNT(*) FROM FBS_Responses GROUP BY form_id")
        result = {r[0]: r[1] for r in cur.fetchall()}
        conn.close()
        return result
    except Exception as e:
        print(f"Error counting responses: {e}")
        return {}


def get_responses_for_analysis(form_id, date_from=None, date_to=None, course_filter=None):
    """
    Return a list of response dicts for analysis.
    Each dict: class_code, course_date, submitted_at, instructor1-3, assessor1-2, answers.
    """
    conn = get_fbs_connection()
    if not conn:
        return []
    try:
        cur    = conn.cursor()
        where  = ["form_id = ?"]
        params = [form_id]
        if date_from:
            where.append("submitted_at >= ?")
            params.append(date_from)
        if date_to:
            where.append("submitted_at < ?")
            params.append(date_to + timedelta(days=1))
        if course_filter:
            where.append("(class_code LIKE ? OR course_title LIKE ?)")
            params.extend([f'%{course_filter}%', f'%{course_filter}%'])
        cur.execute(f"""
            SELECT class_code, course_title, course_date, submitted_at,
                   instructor1_name, instructor2_name, instructor3_name,
                   assessor1_name, assessor2_name, answers_json
            FROM FBS_Responses
            WHERE {' AND '.join(where)}
            ORDER BY submitted_at DESC
        """, params)
        rows = []
        for r in cur.fetchall():
            try:
                answers = json.loads(r[9]) if r[9] else {}
            except (json.JSONDecodeError, TypeError):
                answers = {}
            raw = r[3]
            date_str = raw.strftime('%Y-%m-%d') if isinstance(raw, datetime) else str(raw or '')[:10]
            rows.append({
                'class_code':   str(r[0] or '').strip(),
                'course_title': str(r[1] or '').strip(),
                'course_date':  str(r[2] or '').strip() or date_str,
                'submitted_at': date_str,
                'instructor1':  str(r[4] or ''),
                'instructor2':  str(r[5] or ''),
                'instructor3':  str(r[6] or ''),
                'assessor1':    str(r[7] or ''),
                'assessor2':    str(r[8] or ''),
                'answers':      answers,
            })
        conn.close()
        return rows
    except Exception as e:
        print(f"Error fetching responses for analysis: {e}")
        return []


def get_distinct_courses_for_form(form_id):
    """Return sorted list of distinct class_code values for a form."""
    conn = get_fbs_connection()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT class_code FROM FBS_Responses "
            "WHERE form_id = ? AND class_code IS NOT NULL ORDER BY class_code",
            (form_id,))
        codes = [str(r[0]).strip() for r in cur.fetchall() if r[0]]
        conn.close()
        return codes
    except Exception as e:
        print(f"Error fetching distinct courses: {e}")
        return []


# -- AKC_NAV: course / participant functions -----------------------------------

def get_courses_from_db(search_term=None, limit=50):
    """Fetch courses from AKC_NAV. Returns list of dicts with code and description."""
    conn = get_connection()
    if not conn:
        return []
    try:
        cur   = conn.cursor()
        query = f"SELECT TOP {limit} [Code], [Description] FROM {COURSE_TABLE} WHERE 1=1"
        if search_term:
            query += " AND ([Code] LIKE ? OR [Description] LIKE ?)"
            pat = f'%{search_term}%'
            cur.execute(query + " ORDER BY [Code]", (pat, pat))
        else:
            cur.execute(query + " ORDER BY [Code]")
        courses = [{'code': r[0] or '', 'description': r[1] or ''} for r in cur.fetchall()]
        conn.close()
        return courses
    except Exception as e:
        print(f"Error fetching courses: {e}")
        return []


def get_course_dates():
    """Return list of distinct Registration Dates (YYYY-MM-DD strings) from AKC_NAV."""
    conn = get_connection()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT DISTINCT CAST([Registration Date] AS DATE)
            FROM {PARTICIPANT_TABLE}
            WHERE [Registration Date] IS NOT NULL
            ORDER BY CAST([Registration Date] AS DATE) DESC
        """)
        dates = [str(r[0]) for r in cur.fetchall() if r[0]]
        conn.close()
        return dates
    except Exception as e:
        print(f"Error fetching course dates: {e}")
        return []


def get_class_codes_by_date(registration_date):
    """Return list of distinct Class Codes for a specific Registration Date."""
    conn = get_connection()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT DISTINCT [Class Code]
            FROM {PARTICIPANT_TABLE}
            WHERE CAST([Registration Date] AS DATE) = ?
            AND [Class Code] IS NOT NULL AND [Class Code] != ''
            ORDER BY [Class Code]
        """, (registration_date,))
        codes = [str(r[0]).strip() for r in cur.fetchall() if r[0]]
        conn.close()
        return codes
    except Exception as e:
        print(f"Error fetching class codes by date: {e}")
        return []


def verify_student_participant(class_code, participant_name):
    """Return True if participant_name is registered for class_code (case-insensitive)."""
    conn = get_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT [Participant Name] FROM {PARTICIPANT_TABLE} WHERE [Class Code] = ?",
            (class_code,))
        name_in = ' '.join(participant_name.strip().split()).upper()
        for r in cur.fetchall():
            if ' '.join((r[0] or '').strip().split()).upper() == name_in:
                conn.close()
                return True
        conn.close()
        return False
    except Exception as e:
        print(f"Error verifying participant: {e}")
        return False


def get_participant_name_by_id(class_code, identification_number):
    """Look up participant name by class code + ID number. Returns str or None."""
    conn = get_connection()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT [Participant Name]
            FROM {PARTICIPANT_TABLE}
            WHERE [Class Code] = ?
            AND LTRIM(RTRIM(UPPER([Identification Number]))) = ?
        """, (class_code, identification_number.strip().upper()))
        row = cur.fetchone()
        conn.close()
        return str(row[0]).strip() if row and row[0] else None
    except Exception as e:
        print(f"Error looking up participant by ID: {e}")
        return None


def get_participants_by_class(class_code, offset=0, limit=20):
    """
    Fetch participants for a class with pagination.
    Deduplicates on Identification Number (or Name when ID is absent).
    """
    conn = get_connection()
    if not conn:
        return {'error': 'Could not connect to database'}
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT [Class Code], [Participant Name], [Email Address],
                   [Trainee Designation], [Survey Sent], [Identification Number]
            FROM {PARTICIPANT_TABLE}
            WHERE [Class Code] LIKE ?
            ORDER BY [Participant Name]
        """, (f'%{class_code}%',))
        seen_ids   = set()
        seen_names = set()
        unique     = []
        for r in cur.fetchall():
            id_num = str(r[5]).strip().upper() if r[5] else ''
            name   = str(r[1]).strip().upper() if r[1] else ''
            if id_num:
                if id_num in seen_ids:
                    continue
                seen_ids.add(id_num)
            else:
                if name in seen_names:
                    continue
                if name:
                    seen_names.add(name)
            unique.append({
                'class_code':  r[0] or '',
                'name':        r[1] or '',
                'email':       r[2] or '',
                'designation': r[3] or '',
                'survey_sent': bool(r[4]) if r[4] else False,
                'id_number':   str(r[5]).strip() if r[5] else '',
            })
        conn.close()
        total     = len(unique)
        paginated = unique[offset:offset + limit]
        return {'participants': paginated, 'total': total,
                'offset': offset, 'limit': limit,
                'has_more': (offset + limit) < total}
    except Exception as e:
        print(f"Error fetching participants: {e}")
        return {'error': str(e)}


def update_survey_sent(course_code, participant_name, sent=True):
    """Update the Survey Sent flag for a participant in AKC_NAV."""
    conn = get_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute(f"""
            UPDATE {PARTICIPANT_TABLE}
            SET [Survey Sent] = ?
            WHERE [Course Code] = ? AND [Participant Name] = ?
        """, (1 if sent else 0, course_code, participant_name))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error updating survey sent: {e}")
        return False
