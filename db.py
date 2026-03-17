"""
Database module for SQL Server connection and operations.

AKC_NAV  -- course catalogue and participant lookup (read-mostly).
AKC_FBS  -- feedback form registry (FBS_Forms) and responses (FBS_Responses).
"""

import os
import re
import json
import pyodbc
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

DB_SERVER   = os.getenv('DB_SERVER',   '10.64.2.18')
DB_USERNAME = os.getenv('DB_USERNAME', 'moodleLMSAdmin')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_NAV_DATABASE  = os.getenv('DB_DATABASE',     'AKC_NAV')
COURSE_TABLE      = '[Absolute Kinetics Consultancy$Course]'
PARTICIPANT_TABLE = '[Absolute Kinetics Consultancy$Course Participant]'
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

def _get_table_name(form_title):
    """Convert form title to a SQL table name.
    e.g. 'TRAINER EVALUATION FORM' -> 'trainer_evaluation_form_response'
    """
    slug = re.sub(r'[^a-zA-Z0-9]+', '_', form_title.strip().lower()).strip('_')
    return f"{slug}_response"


_FIXED_COLUMNS_SQL = """\
    [id]               UNIQUEIDENTIFIER NOT NULL,
    [submission_time]  DATETIME2(7) NOT NULL,
    [course_id]        NVARCHAR(20) NOT NULL,
    [course_title]     NVARCHAR(500) NULL,
    [course_date]      DATE NULL,
    [venue]            NVARCHAR(200) NULL,
    [participant_name] NVARCHAR(200) NULL,
    [id_number]        NVARCHAR(100) NULL,
    [position_title]   NVARCHAR(200) NULL"""


def _get_form_columns(form_config):
    """
    Return list of (col_name, sql_type) for all question columns derived from
    the form config sections.

    instructor_rating  -> B{n}_{q_id} INT, B{n}_{q_id}_comment NVARCHAR(500)
    assessor_rating    -> A{n}_{q_id} INT, A{n}_{q_id}_comment NVARCHAR(500)
    rating             -> {q_id} INT,      {q_id}_comment NVARCHAR(500)
    text/mc/yes_no     -> {q_id} NVARCHAR(MAX)
    """
    cols = []
    
    has_instructor_section = any(s.get('type') == 'instructor_rating' for s in form_config.get('sections', []))
    has_assessor_section = any(s.get('type') == 'assessor_rating' for s in form_config.get('sections', []))

    if has_instructor_section:
        max_inst = 3
        for i in range(1, max_inst + 1):
            cols.append((f'instructor{i}_name', 'NVARCHAR(200) NULL'))
            
    if has_assessor_section:
        max_assess = 2 
        for i in range(1, max_assess + 1):
            cols.append((f'assessor{i}_name', 'NVARCHAR(200) NULL'))

    for section in form_config.get('sections', []):
        s_type    = section.get('type', '')
        questions = section.get('questions', [])
        if s_type == 'instructor_rating':
            max_inst = section.get('maxInstructors', 3)
            for i in range(1, max_inst + 1):
                for q in questions:
                    col = f"B{i}_{q['id']}"
                    cols.append((col, 'INT NULL'))
                    cols.append((f"{col}_comment", 'NVARCHAR(500) NULL'))
        elif s_type == 'assessor_rating':
            max_assess = section.get('maxAssessors', 2)
            for i in range(1, max_assess + 1):
                for q in questions:
                    col = f"A{i}_{q['id']}"
                    cols.append((col, 'INT NULL'))
                    cols.append((f"{col}_comment", 'NVARCHAR(500) NULL'))
        elif s_type == 'rating':
            for q in questions:
                cols.append((q['id'], 'INT NULL'))
                cols.append((f"{q['id']}_comment", 'NVARCHAR(500) NULL'))
        elif s_type in ('text_questions', 'multiple_choice', 'yes_no'):
            for q in questions:
                cols.append((q['id'], 'NVARCHAR(MAX) NULL'))
    return cols


def create_form_response_table(form_title, form_config):
    """Create the per-form response table in AKC_FBS if it does not already exist."""
    table = _get_table_name(form_title)
    conn  = get_fbs_connection()
    if not conn:
        return False, "Could not connect to AKC_FBS"
    try:
        cur    = conn.cursor()
        q_cols = _get_form_columns(form_config)
        q_col_sql = '\n'.join(
            f'    [{col}] {sql_type},' for col, sql_type in q_cols
        )
        sql = f"""IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table}' AND xtype='U')
CREATE TABLE [{table}] (
{_FIXED_COLUMNS_SQL},
{q_col_sql}
    CONSTRAINT [PK_{table}] PRIMARY KEY ([id])
)"""
        cur.execute(sql)
        conn.commit()
        conn.close()
        return True, f"Table [{table}] is ready."
    except Exception as e:
        print(f"Error creating table {table}: {e}")
        return False, str(e)


def form_table_exists(form_title):
    """Return True if the per-form response table exists in AKC_FBS."""
    table = _get_table_name(form_title)
    conn  = get_fbs_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM sysobjects WHERE name=? AND xtype='U'",
            (table,))
        exists = cur.fetchone()[0] > 0
        conn.close()
        return exists
    except Exception:
        return False


def sync_form_response_table(form_title, form_config):
    """Sync the per-form response table schema with the current form config.

    - New question columns are ADDED via ALTER TABLE.
    - Removed question columns are KEPT (data is preserved).
    Returns (True, summary_message) or (False, error_message).
    """
    table = _get_table_name(form_title)
    conn  = get_fbs_connection()
    if not conn:
        return False, "Could not connect to AKC_FBS"
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM sysobjects WHERE name=? AND xtype='U'",
            (table,))
        if cur.fetchone()[0] == 0:
            conn.close()
            return create_form_response_table(form_title, form_config)

        cur.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = ? AND TABLE_CATALOG = DB_NAME()",
            (table,))
        existing_cols = {row[0].lower() for row in cur.fetchall()}

        desired_cols = _get_form_columns(form_config)
        added = []
        for col, sql_type in desired_cols:
            if col.lower() not in existing_cols:
                cur.execute(f"ALTER TABLE [{table}] ADD [{col}] {sql_type}")
                added.append(col)

        conn.commit()
        conn.close()
        if added:
            msg = f"Table [{table}] synced. Added {len(added)} column(s): {', '.join(added)}."
        else:
            msg = f"Table [{table}] is up to date (no new columns needed)."
        return True, msg
    except Exception as e:
        print(f"Error syncing table {table}: {e}")
        return False, str(e)


def init_fbs_tables():
    """Create the FBS_Forms registry table if it does not exist."""
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
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='FBS_Courses' AND xtype='U')
            CREATE TABLE FBS_Courses (
                course_id      NVARCHAR(20)  NOT NULL,
                form_id        NVARCHAR(100) NOT NULL,
                course_title   NVARCHAR(500) NOT NULL,
                course_date    NVARCHAR(50)  NULL,
                created_at     DATETIME      NOT NULL DEFAULT GETDATE(),
                is_active      BIT           NOT NULL DEFAULT 1,
                deactivated_at DATETIME      NULL,
                extra_fields   NVARCHAR(MAX) NULL,
                CONSTRAINT PK_FBS_Courses PRIMARY KEY (course_id)
            )
        """)
        conn.commit()
        conn.close()
        return True, "FBS_Forms and FBS_Courses ready."
    except Exception as e:
        return False, f"Error initialising FBS tables: {e}"

def _course_row_to_dict(row):
    """Convert a FBS_Courses DB row to a plain dict identical to the old config format."""
    d = {
        'id':           row[0],
        'form_id':      row[1],
        'course_title': row[2],
        'course_date':  row[3] or '',
        'created_at':   row[4].strftime('%Y-%m-%d %H:%M:%S') if isinstance(row[4], datetime) else str(row[4] or ''),
        'is_active':    bool(row[5]),
        'deactivated_at': row[6].strftime('%Y-%m-%d %H:%M:%S') if isinstance(row[6], datetime) else None,
    }
    if row[7]:
        try:
            d.update(json.loads(row[7]))
        except (json.JSONDecodeError, TypeError):
            pass
    return d


def create_course_in_db(course_dict):
    """Insert a new course session into FBS_Courses."""
    conn = get_fbs_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        extra = {k: v for k, v in course_dict.items()
                 if k not in ('id', 'form_id', 'course_title', 'course_date', 'created_at', 'is_active')}
        cur.execute(
            "INSERT INTO FBS_Courses (course_id, form_id, course_title, course_date, extra_fields) "
            "VALUES (?, ?, ?, ?, ?)",
            (course_dict['id'], course_dict['form_id'],
             course_dict['course_title'], course_dict.get('course_date', ''),
             json.dumps(extra, ensure_ascii=False)))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error creating course in DB: {e}")
        return False


def get_course_by_id(course_id):
    """Return a single course dict, or None if not found."""
    conn = get_fbs_connection()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT course_id, form_id, course_title, course_date, created_at, "
            "is_active, deactivated_at, extra_fields "
            "FROM FBS_Courses WHERE course_id = ?", (course_id,))
        row = cur.fetchone()
        conn.close()
        return _course_row_to_dict(row) if row else None
    except Exception as e:
        print(f"Error getting course {course_id}: {e}")
        return None


def get_all_courses_from_db():
    """Return all course sessions (active and closed), newest first."""
    conn = get_fbs_connection()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT course_id, form_id, course_title, course_date, created_at, "
            "is_active, deactivated_at, extra_fields "
            "FROM FBS_Courses ORDER BY created_at DESC")
        rows = cur.fetchall()
        conn.close()
        return [_course_row_to_dict(r) for r in rows]
    except Exception as e:
        print(f"Error getting all courses: {e}")
        return []


def get_active_courses_by_title(course_title):
    """Return all ACTIVE course sessions matching the given course_title."""
    conn = get_fbs_connection()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT course_id, form_id, course_title, course_date, created_at, "
            "is_active, deactivated_at, extra_fields "
            "FROM FBS_Courses WHERE is_active = 1 AND course_title = ?",
            (course_title,))
        rows = cur.fetchall()
        conn.close()
        return [_course_row_to_dict(r) for r in rows]
    except Exception as e:
        print(f"Error getting active courses by title: {e}")
        return []


def get_courses_by_title(course_title):
    """Return ALL (active + closed) course sessions matching the given course_title."""
    conn = get_fbs_connection()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT course_id, form_id, course_title, course_date, created_at, "
            "is_active, deactivated_at, extra_fields "
            "FROM FBS_Courses WHERE course_title = ?",
            (course_title,))
        rows = cur.fetchall()
        conn.close()
        return [_course_row_to_dict(r) for r in rows]
    except Exception as e:
        print(f"Error getting courses by title: {e}")
        return []


def deactivate_course(course_id):
    """Close a QR session (is_active → 0). The record is kept for data integrity."""
    conn = get_fbs_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE FBS_Courses SET is_active = 0, deactivated_at = GETDATE() "
            "WHERE course_id = ?", (course_id,))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error deactivating course {course_id}: {e}")
        return False


def reactivate_course(course_id):
    """Re-open a previously closed QR session."""
    conn = get_fbs_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE FBS_Courses SET is_active = 1, deactivated_at = NULL "
            "WHERE course_id = ?", (course_id,))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error reactivating course {course_id}: {e}")
        return False

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


def form_has_responses(form_id, form_title):
    """Return True if the per-form response table has at least one row."""
    table = _get_table_name(form_title)
    conn  = get_fbs_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT TOP 1 1 FROM [{table}]")
        has_row = cur.fetchone() is not None
        conn.close()
        return has_row
    except Exception:
        return False


def drop_form_response_table_if_empty(form_title):
    """Drop the per-form response table only when it exists and has zero rows.

    Returns (ok, dropped, message):
      - ok=True, dropped=True  : table dropped
      - ok=True, dropped=False : table not dropped (not found or contains rows)
      - ok=False, dropped=False: unexpected DB error
    """
    table = _get_table_name(form_title)
    conn = get_fbs_connection()
    if not conn:
        return False, False, "Could not connect to AKC_FBS"
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM sysobjects WHERE name=? AND xtype='U'",
            (table,))
        if cur.fetchone()[0] == 0:
            conn.close()
            return True, False, f"Table [{table}] does not exist."

        cur.execute(f"SELECT COUNT(*) FROM [{table}]")
        row_count = int(cur.fetchone()[0] or 0)
        if row_count > 0:
            conn.close()
            return True, False, f"Table [{table}] contains {row_count} row(s); keeping table."

        cur.execute(f"DROP TABLE [{table}]")
        conn.commit()
        conn.close()
        return True, True, f"Table [{table}] dropped (empty table)."
    except Exception as e:
        print(f"Error dropping empty response table [{table}]: {e}")
        return False, False, str(e)

def has_submitted_db(course_id, id_number, form_title):
    """Return True if a response already exists in the form's per-form table."""
    table = _get_table_name(form_title)
    conn  = get_fbs_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT COUNT(*) FROM [{table}] "
            "WHERE course_id = ? AND UPPER(LTRIM(RTRIM(id_number))) = ?",
            (course_id, id_number.strip().upper()))
        row = cur.fetchone()
        conn.close()
        return bool(row and row[0] > 0)
    except Exception as e:
        print(f"Error checking submission in [{table}]: {e}")
        return False

def get_submitted_ids_for_courses(course_ids, form_titles):
    """
    Return a set of normalised id_numbers that have already submitted
    for any of the given course_ids, checking all relevant per-form tables.
    """
    if not course_ids or not form_titles:
        return set()
    conn = get_fbs_connection()
    if not conn:
        return set()
    ids = set()
    try:
        cur = conn.cursor()
        ph  = ','.join('?' for _ in course_ids)
        for form_title in set(form_titles):
            table = _get_table_name(form_title)
            try:
                cur.execute(
                    f"SELECT UPPER(LTRIM(RTRIM(id_number))) FROM [{table}] "
                    f"WHERE course_id IN ({ph})",
                    list(course_ids))
                ids.update(r[0] for r in cur.fetchall() if r[0])
            except Exception:
                pass
        conn.close()
        return ids
    except Exception as e:
        print(f"Error fetching submitted IDs: {e}")
        return ids


def save_response_to_db(form_id, course_id, course, participant_name,
                        id_number, position, data, form_title, form_config):
    """
    Insert one response into the per-form response table.
    course      : course dict (course_title, course_date, classroom/venue,
                  instructors, assessors).
    data        : raw submitted dict from the form.
    form_title  : used to derive the table name.
    form_config : used to build the column list.
    """
    table = _get_table_name(form_title)
    conn  = get_fbs_connection()
    if not conn:
        return False
    try:
        cur         = conn.cursor()
        instructors = course.get('instructors', [])
        assessors   = course.get('assessors',   [])
        venue       = (course.get('classroom') or
                       course.get('assessment_location') or
                       course.get('venue') or '')

        fixed_col_names = [
            'course_id', 'course_title', 'course_date', 'venue',
            'participant_name', 'id_number', 'position_title',
            'instructor1_name', 'instructor2_name', 'instructor3_name',
            'assessor1_name',   'assessor2_name',
        ]
        fixed_vals = [
            course_id,
            course.get('course_title', ''),
            course.get('course_date', ''),
            venue,
            participant_name or '',
            (id_number or '').strip().upper(),
            position or '',
            instructors[0] if len(instructors) > 0 else None,
            instructors[1] if len(instructors) > 1 else None,
            instructors[2] if len(instructors) > 2 else None,
            assessors[0]   if len(assessors)   > 0 else None,
            assessors[1]   if len(assessors)   > 1 else None,
        ]

        q_cols      = _get_form_columns(form_config)
        q_col_names = [col for col, _ in q_cols]
        q_vals      = []
        for col, sql_type in q_cols:
            val = data.get(col)
            if val is not None and 'INT' in sql_type:
                try:
                    val = int(val)
                except (ValueError, TypeError):
                    val = None
            elif val is not None:
                val = str(val)
            q_vals.append(val)

        all_col_names = fixed_col_names + q_col_names
        all_vals      = fixed_vals + q_vals
        cols_sql      = ', '.join(f'[{c}]' for c in all_col_names)
        params_sql    = ', '.join('?' for _ in all_vals)

        cur.execute(
            f"INSERT INTO [{table}] ({cols_sql}) VALUES ({params_sql})",
            all_vals)
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error saving response to [{table}]: {e}")
        return False

def get_response_count_by_form(forms_dict):
    """Return {form_id: count} by querying each form's per-form response table."""
    result = {}
    conn   = get_fbs_connection()
    if not conn:
        return result
    try:
        cur = conn.cursor()
        for form_id, form_config in forms_dict.items():
            table = _get_table_name(form_config.get('title', form_id))
            try:
                cur.execute(f"SELECT COUNT(*) FROM [{table}]")
                result[form_id] = cur.fetchone()[0]
            except Exception:
                result[form_id] = 0
        conn.close()
        return result
    except Exception as e:
        print(f"Error counting responses: {e}")
        return result


def get_responses_for_analysis(form_id, form_config, date_from=None, date_to=None, course_filter=None):
    """
    Return response rows from the per-form table for analysis.
    Each row: class_code, course_title, course_date, submitted_at,
              instructor1..3_name, assessor1..2_name, answers (col→val dict).
    """
    table  = _get_table_name(form_config.get('title', form_id))
    conn   = get_fbs_connection()
    if not conn:
        return []
    try:
        cur    = conn.cursor()
        where  = ['1=1']
        params = []
        if date_from:
            where.append('submission_time >= ?')
            params.append(date_from)
        if date_to:
            where.append('submission_time < ?')
            params.append(date_to + timedelta(days=1))
        if course_filter:
            where.append('(course_id LIKE ? OR course_title LIKE ?)')
            params.extend([f'%{course_filter}%', f'%{course_filter}%'])

        q_cols      = _get_form_columns(form_config)
        q_col_names = [col for col, _ in q_cols]
        q_col_sql   = (', ' + ', '.join(f'[{c}]' for c in q_col_names)) if q_col_names else ''

        cur.execute(f"""
            SELECT course_id, course_title, course_date, submission_time,
                   instructor1_name, instructor2_name, instructor3_name,
                   assessor1_name, assessor2_name{q_col_sql}
            FROM [{table}]
            WHERE {' AND '.join(where)}
            ORDER BY submission_time DESC
        """, params)

        fixed_keys = ['course_id', 'course_title', 'course_date', 'submission_time',
                      'instructor1_name', 'instructor2_name', 'instructor3_name',
                      'assessor1_name', 'assessor2_name']
        all_keys   = fixed_keys + q_col_names
        rows = []
        for r in cur.fetchall():
            row_dict = dict(zip(all_keys, r))
            answers  = {col: row_dict.pop(col) for col in q_col_names}
            raw      = row_dict.get('submission_time')
            date_str = raw.strftime('%Y-%m-%d') if isinstance(raw, datetime) else str(raw or '')[:10]
            rows.append({
                'class_code':   str(row_dict.get('course_id',    '') or '').strip(),
                'course_title': str(row_dict.get('course_title', '') or '').strip(),
                'course_date':  str(row_dict.get('course_date',  '') or '').strip() or date_str,
                'submitted_at': date_str,
                'instructor1':  str(row_dict.get('instructor1_name') or ''),
                'instructor2':  str(row_dict.get('instructor2_name') or ''),
                'instructor3':  str(row_dict.get('instructor3_name') or ''),
                'assessor1':    str(row_dict.get('assessor1_name')   or ''),
                'assessor2':    str(row_dict.get('assessor2_name')   or ''),
                'answers':      answers,
            })
        conn.close()
        return rows
    except Exception as e:
        print(f"Error fetching responses from [{table}]: {e}")
        return []


def get_distinct_courses_for_form(form_id, form_config):
    """Return sorted list of distinct course_id values in the per-form table."""
    table = _get_table_name(form_config.get('title', form_id))
    conn  = get_fbs_connection()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT DISTINCT course_id FROM [{table}] "
            "WHERE course_id IS NOT NULL ORDER BY course_id")
        codes = [str(r[0]).strip() for r in cur.fetchall() if r[0]]
        conn.close()
        return sorted(codes)
    except Exception as e:
        print(f"Error fetching distinct courses from [{table}]: {e}")
        return []

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
