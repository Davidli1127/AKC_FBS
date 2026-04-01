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
    """Convert form title to a SQL table name."""
    slug = re.sub(r'[^a-zA-Z0-9]+', '_', form_title.strip().lower()).strip('_')
    return f"{slug}_response"


_FIXED_COLUMNS_SQL = """\
    [id]               UNIQUEIDENTIFIER NOT NULL,
    [submission_time]  DATETIME2(7) NOT NULL,
    [course_id]        NVARCHAR(20) NOT NULL,
    [course_title]     NVARCHAR(500) NULL,
    [course_code]      NVARCHAR(50) NULL,
    [course_date]      DATE NULL,
    [venue]            NVARCHAR(200) NULL,
    [language]         NVARCHAR(50) NULL DEFAULT 'English',
    [participant_name] NVARCHAR(200) NULL,
    [id_number]        NVARCHAR(100) NULL,
    [position_title]   NVARCHAR(200) NULL"""


def _get_form_columns(form_config):
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
        
        # Check for missing fixed columns
        fixed_cols = [
            ('course_code', 'NVARCHAR(50) NULL'),
        ]
        
        for col, sql_type in fixed_cols:
            if col.lower() not in existing_cols:
                cur.execute(f"ALTER TABLE [{table}] ADD [{col}] {sql_type}")
                added.append(col)
        
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


def get_fbs_course_title_map(course_ids):
    """Return {course_id: course_title} from FBS_Courses for a list of course IDs."""
    ids = sorted({str(c).strip() for c in (course_ids or []) if str(c).strip()})
    if not ids:
        return {}

    conn = get_fbs_connection()
    if not conn:
        return {}

    try:
        placeholders = ','.join('?' for _ in ids)
        cur = conn.cursor()
        cur.execute(
            f"SELECT course_id, course_title FROM FBS_Courses WHERE course_id IN ({placeholders})",
            ids
        )
        mapping = {}
        for row in cur.fetchall():
            cid = str(row[0] or '').strip()
            ctitle = str(row[1] or '').strip()
            if cid:
                mapping[cid] = ctitle
        conn.close()
        return mapping
    except Exception as e:
        print(f"Error fetching FBS course title map: {e}")
        return {}


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

def register_form(form_id, form_title, form_number, description, config_dict, language='English'):
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
                SET form_title = ?, language = ?, form_number = ?, description = ?,
                    config_json = ?, updated_at = ?, is_deleted = 0, deleted_at = NULL
                WHERE form_id = ?
            """, (form_title, language, form_number or '', description or '', cfg, now, form_id))
        else:
            cur.execute("""
                INSERT INTO FBS_Forms
                    (form_id, form_title, language, form_number, description, config_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (form_id, form_title, language, form_number or '', description or '', cfg, now, now))
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


def get_active_forms_map():
    """Return active (not soft-deleted) forms as {form_id: config_dict}."""
    conn = get_fbs_connection()
    if not conn:
        return {}
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT form_id, form_title, form_number, description, config_json "
            "FROM FBS_Forms WHERE is_deleted = 0"
        )
        rows = cur.fetchall()
        conn.close()

        forms = {}
        for row in rows:
            form_id = row[0]
            form_title = row[1] or form_id
            form_number = row[2] or ''
            description = row[3] or ''
            config_json = row[4]

            cfg = {}
            if config_json:
                try:
                    cfg = json.loads(config_json)
                except Exception:
                    cfg = {}

            if not isinstance(cfg, dict):
                cfg = {}

            cfg['id'] = cfg.get('id', form_id)
            cfg['title'] = cfg.get('title', form_title)
            cfg['formNumber'] = cfg.get('formNumber', form_number)
            cfg['description'] = cfg.get('description', description)
            cfg['sections'] = cfg.get('sections', [])
            cfg['headerFields'] = cfg.get('headerFields', [])
            cfg['ratingOptions'] = cfg.get('ratingOptions', [])
            cfg['language'] = cfg.get('language', 'English')

            forms[form_id] = cfg

        return forms
    except Exception as e:
        print(f"Error getting active forms: {e}")
        return {}


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
    """Drop the per-form response table only when it exists and has zero rows. """
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

def save_response_to_db(form_id, course_id, course, participant_name, id_number, position, data, form_title, form_config, language='English'):
    table = _get_table_name(form_title)
    conn  = get_fbs_connection()
    if not conn:
        print(f"Error: Cannot connect to database for table [{table}]")
        return False
    try:
        cur         = conn.cursor()
        instructors = course.get('instructors', [])
        assessors   = course.get('assessors',   [])
        venue       = (course.get('classroom') or
                       course.get('assessment_location') or
                       course.get('venue') or '')
        
        # Look up Course Code from NAV based on Class Code
        class_code = course.get('course_title', '')
        course_code = get_course_code_for_class_code(class_code) or ''

        fixed_col_names = [
            'course_id', 'course_title', 'course_code', 'course_date', 'venue', 'language',
            'participant_name', 'id_number', 'position_title',
            'instructor1_name', 'instructor2_name', 'instructor3_name',
            'assessor1_name',   'assessor2_name',
        ]
        fixed_vals = [
            course_id,
            class_code,
            course_code,
            course.get('course_date', ''),
            venue,
            language,
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

        fixed_col_set = set(fixed_col_names)
        deduplicated_cols = []
        deduplicated_vals = []
        for col, val in zip(q_col_names, q_vals):
            if col not in fixed_col_set:
                deduplicated_cols.append(col)
                deduplicated_vals.append(val)
        
        all_col_names = fixed_col_names + deduplicated_cols
        all_vals      = fixed_vals + deduplicated_vals
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
    """Return response rows from the per-form table for analysis."""
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
            SELECT course_id, course_title, course_code, course_date, submission_time,
                   instructor1_name, instructor2_name, instructor3_name,
                   assessor1_name, assessor2_name{q_col_sql}
            FROM [{table}]
            WHERE {' AND '.join(where)}
            ORDER BY submission_time DESC
        """, params)

        fixed_keys = ['course_id', 'course_title', 'course_code', 'course_date', 'submission_time',
                      'instructor1_name', 'instructor2_name', 'instructor3_name',
                      'assessor1_name', 'assessor2_name']
        all_keys   = fixed_keys + q_col_names
        rows = []
        for r in cur.fetchall():
            row_dict = dict(zip(all_keys, r))
            answers  = {col: row_dict.pop(col) for col in q_col_names}
            raw      = row_dict.get('submission_time')
            date_str = raw.strftime('%Y-%m-%d') if isinstance(raw, datetime) else str(raw or '')[:10]
            raw_course_id = str(row_dict.get('course_id', '') or '').strip()
            raw_course_title = str(row_dict.get('course_title', '') or '').strip()
            raw_course_code = str(row_dict.get('course_code', '') or '').strip()

            rows.append({
                'course_code':  raw_course_code,
                'course_title': raw_course_title,
                'course_id':    raw_course_id,
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


def get_available_analysis_months(form_id, form_config):
    """Return distinct available months (YYYY-MM) from a form response table."""
    table = _get_table_name(form_config.get('title', form_id))
    conn = get_fbs_connection()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT DISTINCT
                YEAR([submission_time]) AS y,
                MONTH([submission_time]) AS m
            FROM [{table}]
            WHERE [submission_time] IS NOT NULL
            ORDER BY y DESC, m DESC
        """)
        months = [f"{int(r[0]):04d}-{int(r[1]):02d}" for r in cur.fetchall() if r[0] and r[1]]
        conn.close()
        return months
    except Exception as e:
        print(f"Error fetching analysis months from [{table}]: {e}")
        return []


def get_nav_course_name_map(class_codes):
    if not class_codes:
        return {}

    codes = sorted({str(c).strip().upper() for c in class_codes if str(c).strip()})
    if not codes:
        return {}

    conn = get_connection()
    if not conn:
        return {}

    try:
        placeholders = ','.join('?' for _ in codes)
        cur = conn.cursor()
        cur.execute(f"""
            WITH req AS (
                SELECT ? AS class_code
                {''.join(' UNION ALL SELECT ?' for _ in range(len(codes) - 1))}
            ), mapped AS (
                SELECT
                    req.class_code,
                    NULLIF(LTRIM(RTRIM(c_ts.[Name])), '') AS course_name,
                    1 AS source_rank,
                    COUNT(*) AS hit_count
                FROM req
                LEFT JOIN {PARTICIPANT_TABLE} p
                    ON UPPER(LTRIM(RTRIM(p.[Class Code]))) = req.class_code
                LEFT JOIN {COURSE_TABLE} c_ts
                    ON p.[Timestamp] = c_ts.[Timestamp]
                GROUP BY req.class_code, NULLIF(LTRIM(RTRIM(c_ts.[Name])), '')

                UNION ALL

                SELECT
                    req.class_code,
                    NULLIF(LTRIM(RTRIM(c_code.[Name])), '') AS course_name,
                    2 AS source_rank,
                    COUNT(*) AS hit_count
                FROM req
                LEFT JOIN {PARTICIPANT_TABLE} p
                    ON UPPER(LTRIM(RTRIM(p.[Class Code]))) = req.class_code
                LEFT JOIN {COURSE_TABLE} c_code
                    ON UPPER(LTRIM(RTRIM(p.[Class Code]))) = UPPER(LTRIM(RTRIM(c_code.[Code])))
                GROUP BY req.class_code, NULLIF(LTRIM(RTRIM(c_code.[Name])), '')

                UNION ALL

                SELECT
                    req.class_code,
                    NULLIF(LTRIM(RTRIM(c_direct.[Name])), '') AS course_name,
                    3 AS source_rank,
                    COUNT(*) AS hit_count
                FROM req
                LEFT JOIN {COURSE_TABLE} c_direct
                    ON UPPER(LTRIM(RTRIM(c_direct.[Code]))) = req.class_code
                GROUP BY req.class_code, NULLIF(LTRIM(RTRIM(c_direct.[Name])), '')
            ), ranked AS (
                SELECT
                    class_code,
                    course_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY class_code
                        ORDER BY CASE WHEN course_name IS NULL THEN 1 ELSE 0 END,
                                 source_rank ASC,
                                 hit_count DESC,
                                 course_name ASC
                    ) AS rn
                FROM mapped
            )
            SELECT class_code, course_name
            FROM ranked
            WHERE rn = 1
        """, codes)

        result = {}
        for row in cur.fetchall():
            class_code = str(row[0] or '').strip().upper()
            course_name = str(row[1] or '').strip()
            if class_code:
                result[class_code] = course_name

        conn.close()
        return result
    except Exception as e:
        print(f"Error mapping NAV course names by class code: {e}")
        return {}

def add_course_code_column(form_title):
    """Add course_code column to form table if it doesn't exist."""
    table = _get_table_name(form_title)
    conn  = get_fbs_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        
        # Check if column exists
        cur.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = ? AND COLUMN_NAME = 'course_code'
        """, (table,))
        
        if cur.fetchone()[0] == 0:
            # Column doesn't exist, add it
            cur.execute(f"ALTER TABLE [{table}] ADD [course_code] NVARCHAR(50) NULL")
            conn.commit()
            print(f"Added course_code column to [{table}]")
        
        conn.close()
        return True
    except Exception as e:
        print(f"Error adding course_code column to {table}: {e}")
        return False

def backfill_course_codes(form_title):
    """Populate course_code column for existing responses using their class codes."""
    table = _get_table_name(form_title)
    conn  = get_fbs_connection()
    if not conn:
        return False, "Could not connect to AKC_FBS"
    
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT [id], [course_title] 
            FROM [{table}]
            WHERE [course_code] IS NULL OR [course_code] = ''
        """)
        
        rows = cur.fetchall()
        updated = 0
        
        for row_id, class_code in rows:
            if not class_code:
                continue
            
            # Look up course code from NAV
            course_code = get_course_code_for_class_code(class_code)
            if course_code:
                cur.execute(f"""
                    UPDATE [{table}]
                    SET [course_code] = ?
                    WHERE [id] = ?
                """, (course_code, row_id))
                updated += 1
        
        conn.commit()
        conn.close()
        return True, f"Updated {updated} responses with course codes"
    except Exception as e:
        print(f"Error backfilling course codes for {table}: {e}")
        return False, str(e)

def get_course_code_for_class_code(class_code):
    """Get the Course Code from NAV for a given Class Code. Returns course_code or None."""
    if not class_code:
        return None
    
    class_code = str(class_code or '').strip()
    if not class_code:
        return None
    
    conn = get_connection()
    if not conn:
        print(f"NAV connection failed for class_code lookup: {class_code}")
        return None
    
    try:
        cur = conn.cursor()
        sql = f"SELECT TOP 1 [{chr(91)}Course Code{chr(93)}] FROM {PARTICIPANT_TABLE} WHERE UPPER(LTRIM(RTRIM([{chr(91)}Class Code{chr(93)}]))) = ?"
        query = f"""
            SELECT TOP 1 [Course Code]
            FROM {PARTICIPANT_TABLE}
            WHERE UPPER(RTRIM(LTRIM([Class Code]))) = ?
        """
        
        print(f"DEBUG: Executing query for class_code: {class_code}")
        cur.execute(query, (class_code.upper().strip(),))
        
        row = cur.fetchone()
        conn.close()
        
        if row:
            result = str(row[0] or '').strip()
            print(f"DEBUG: Found course code '{result}' for class code '{class_code}'")
            return result if result else None
        else:
            print(f"DEBUG: No match found for class code '{class_code}'")
            return None
    except Exception as e:
        print(f"Error getting course code for class code '{class_code}': {e}")
        return None

def get_nav_course_code_map(class_codes):
    """Get Course Code from NAV database by Class Code. Returns {class_code_upper: course_code}"""
    if not class_codes:
        return {}

    codes = sorted({str(c).strip().upper() for c in class_codes if str(c).strip()})
    if not codes:
        return {}

    conn = get_connection()
    if not conn:
        return {}

    try:
        placeholders = ','.join('?' for _ in codes)
        cur = conn.cursor()
        cur.execute(f"""
            WITH req AS (
                SELECT ? AS class_code
                {''.join(' UNION ALL SELECT ?' for _ in range(len(codes) - 1))}
            ), mapped AS (
                SELECT
                    req.class_code,
                    NULLIF(LTRIM(RTRIM(c_ts.[Code])), '') AS course_code,
                    1 AS source_rank,
                    COUNT(*) AS hit_count
                FROM req
                LEFT JOIN {PARTICIPANT_TABLE} p
                    ON UPPER(LTRIM(RTRIM(p.[Class Code]))) = req.class_code
                LEFT JOIN {COURSE_TABLE} c_ts
                    ON p.[Timestamp] = c_ts.[Timestamp]
                GROUP BY req.class_code, NULLIF(LTRIM(RTRIM(c_ts.[Code])), '')

                UNION ALL

                SELECT
                    req.class_code,
                    NULLIF(LTRIM(RTRIM(c_code.[Code])), '') AS course_code,
                    2 AS source_rank,
                    COUNT(*) AS hit_count
                FROM req
                LEFT JOIN {PARTICIPANT_TABLE} p
                    ON UPPER(LTRIM(RTRIM(p.[Class Code]))) = req.class_code
                LEFT JOIN {COURSE_TABLE} c_code
                    ON UPPER(LTRIM(RTRIM(p.[Class Code]))) = UPPER(LTRIM(RTRIM(c_code.[Code])))
                GROUP BY req.class_code, NULLIF(LTRIM(RTRIM(c_code.[Code])), '')

                UNION ALL

                SELECT
                    req.class_code,
                    NULLIF(LTRIM(RTRIM(c_direct.[Code])), '') AS course_code,
                    3 AS source_rank,
                    COUNT(*) AS hit_count
                FROM req
                LEFT JOIN {COURSE_TABLE} c_direct
                    ON UPPER(LTRIM(RTRIM(c_direct.[Code]))) = req.class_code
                GROUP BY req.class_code, NULLIF(LTRIM(RTRIM(c_direct.[Code])), '')
            ), ranked AS (
                SELECT
                    class_code,
                    course_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY class_code
                        ORDER BY CASE WHEN course_code IS NULL THEN 1 ELSE 0 END,
                                 source_rank ASC,
                                 hit_count DESC,
                                 course_code ASC
                    ) AS rn
                FROM mapped
            )
            SELECT class_code, course_code
            FROM ranked
            WHERE rn = 1
        """, codes)

        result = {}
        for row in cur.fetchall():
            class_code = str(row[0] or '').strip().upper()
            course_code = str(row[1] or '').strip()
            if class_code:
                result[class_code] = course_code

        conn.close()
        return result
    except Exception as e:
        print(f"Error mapping NAV course codes by class code: {e}")
        return {}

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
    """Return True if participant_name is registered for class_code."""
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
    """Fetch participants for a class with pagination. Deduplicates on Identification Number"""
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

def init_rectification_log_table():
    """Create the FBS_Rectification_Log table if it does not exist."""
    conn = get_fbs_connection()
    if not conn:
        return False, "Could not connect to AKC_FBS"
    try:
        cur = conn.cursor()
        cur.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='FBS_Rectification_Log' AND xtype='U')
            CREATE TABLE FBS_Rectification_Log (
                log_id              UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
                form_id             NVARCHAR(100) NOT NULL,
                response_id         UNIQUEIDENTIFIER NOT NULL,
                participant_name    NVARCHAR(200) NOT NULL,
                participant_email   NVARCHAR(200) NOT NULL,
                question_id         NVARCHAR(100) NOT NULL,
                question_text       NVARCHAR(MAX) NOT NULL,
                rating_value        INT NOT NULL,
                rectification_text  NVARCHAR(MAX) NOT NULL,
                implementation_date DATE NULL,
                status              NVARCHAR(50) NOT NULL DEFAULT 'Pending',
                email_sent_at       DATETIME NOT NULL DEFAULT GETDATE(),
                created_at          DATETIME NOT NULL DEFAULT GETDATE()
            )
        """)
        conn.commit()
        conn.close()
        return True, "FBS_Rectification_Log ready."
    except Exception as e:
        print(f"Error initializing FBS_Rectification_Log: {e}")
        return False, str(e)


def get_low_rating_responses(form_id, form_config, rating_threshold=2):
    """Get all low-rated responses (rating <= rating_threshold) from a form."""
    table = _get_table_name(form_config.get('title', form_id))
    conn = get_fbs_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = ? AND TABLE_CATALOG = DB_NAME()
        """, (table,))
        all_cols = set(row[0] for row in cur.fetchall())
        col_to_q_map = {}
        
        for section in form_config.get('sections', []):
            section_type = section.get('type', '')
            if section_type == 'rating':
                for q in section.get('questions', []):
                    q_id = q.get('id')
                    if q_id in all_cols:
                        col_to_q_map[q_id] = {
                            'question_id': q_id,
                            'text': q.get('text', ''),
                            'section_type': section_type
                        }
            elif section_type == 'instructor_rating':
                max_inst = section.get('maxInstructors', 3)
                for n in range(1, max_inst + 1):
                    for q in section.get('questions', []):
                        q_id = q.get('id')
                        col_name = f"B{n}_{q_id}"
                        if col_name in all_cols:
                            col_to_q_map[col_name] = {
                                'question_id': col_name,
                                'text': q.get('text', ''),
                                'section_type': section_type,
                                'instructor_num': n
                            }
            elif section_type == 'assessor_rating':
                max_assess = section.get('maxAssessors', 2)
                for n in range(1, max_assess + 1):
                    for q in section.get('questions', []):
                        q_id = q.get('id')
                        col_name = f"A{n}_{q_id}"
                        if col_name in all_cols:
                            col_to_q_map[col_name] = {
                                'question_id': col_name,
                                'text': q.get('text', ''),
                                'section_type': section_type,
                                'assessor_num': n
                            }
        
        rating_cols = list(col_to_q_map.keys())
        if not rating_cols:
            return []
        
        rating_col_sql = ', '.join(f'[{col}]' for col in rating_cols)
        where_clauses = ' OR '.join(f'[{col}] <= {rating_threshold}' for col in rating_cols)
        
        cur.execute(f"""
            SELECT 
                [id], 
                [participant_name], 
                [id_number],
                [submission_time],
                [course_id],
                [course_title],
                {rating_col_sql}
            FROM [{table}]
            WHERE ({where_clauses})
            ORDER BY [submission_time] DESC
        """)
        
        responses = []
        for row in cur.fetchall():
            row_list = list(row)
            response_id = row_list[0]
            participant_name = row_list[1]
            id_number = row_list[2]
            submission_time = row_list[3]
            course_id = row_list[4]
            course_title = row_list[5]
            ratings_values = row_list[6:]
            participant_email = _get_participant_email(course_title, id_number, participant_name) or "N/A"
            
            low_ratings = []
            for i, col_name in enumerate(rating_cols):
                rating_val = ratings_values[i]
                if rating_val is not None and rating_val <= rating_threshold:
                    q_info = col_to_q_map.get(col_name, {})
                    low_ratings.append({
                        'question_id': q_info.get('question_id', col_name),
                        'question_text': q_info.get('text', ''),
                        'rating_value': rating_val,
                        'section_type': q_info.get('section_type', '')
                    })
            
            if low_ratings:
                responses.append({
                    'response_id': response_id,
                    'participant_name': participant_name or '',
                    'participant_email': participant_email,
                    'id_number': id_number or '',
                    'form_id': form_id,
                    'form_title': form_config.get('title', form_id),
                    'course_id': course_id or '',
                    'course_title': course_title or '',
                    'submission_time': submission_time,
                    'ratings': low_ratings
                })
        
        conn.close()
        return responses
    except Exception as e:
        print(f"Error getting low rating responses: {e}")
        return []

def _get_participant_email(course_id, id_number, participant_name):
    """Look up participant email from AKC_NAV using course_id and id_number."""
    try:
        conn = get_connection()
        if not conn:
            return None
        cur = conn.cursor()
        if id_number and id_number.strip():
            cur.execute(f"""
                SELECT [Email Address]
                FROM {PARTICIPANT_TABLE}
                WHERE [Class Code] = ? AND LTRIM(RTRIM(UPPER([Identification Number]))) = ?
            """, (course_id, id_number.strip().upper()))
        else:
            cur.execute(f"""
                SELECT [Email Address]
                FROM {PARTICIPANT_TABLE}
                WHERE [Class Code] = ? AND [Participant Name] = ?
            """, (course_id, participant_name))
        row = cur.fetchone()
        conn.close()
        return str(row[0]).strip() if row and row[0] else None
    except Exception as e:
        print(f"Error getting participant email: {e}")
        return None


def get_all_rating_questions_by_form(forms_dict):
    result = {}
    for form_id, form_config in forms_dict.items():
        questions = []
        for section in form_config.get('sections', []):
            if section.get('type') in ('rating', 'instructor_rating', 'assessor_rating'):
                section_type = section.get('type')
                for q in section.get('questions', []):
                    questions.append({
                        'id': q.get('id'),
                        'text': q.get('text', ''),
                        'type': section_type
                    })
        if questions:
            result[form_id] = questions
    return result

def log_rectification_sent(form_id, response_id, participant_name, participant_email,
                           question_id, question_text, rating_value, rectification_text,
                           implementation_date, status):
    """Log a sent rectification to prevent duplicates."""
    conn = get_fbs_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO FBS_Rectification_Log 
            (form_id, response_id, participant_name, participant_email, question_id,
             question_text, rating_value, rectification_text, implementation_date, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (form_id, response_id, participant_name, participant_email, question_id,
              question_text, rating_value, rectification_text, implementation_date, status))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error logging rectification: {e}")
        return False

def check_rectification_already_sent(form_id, response_id, question_id):
    """Check if a rectification has already been sent for this low rating."""
    conn = get_fbs_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM FBS_Rectification_Log
            WHERE form_id = ? AND response_id = ? AND question_id = ?
        """, (form_id, response_id, question_id))
        count = cur.fetchone()[0]
        conn.close()
        return count > 0
    except Exception:
        return False

def get_text_question_responses(form_id, form_config):
    """Get all text/short-answer question responses for alert management."""
    table = _get_table_name(form_config.get('title', form_id))
    conn = get_fbs_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = ? AND TABLE_CATALOG = DB_NAME()
        """, (table,))
        all_cols = [row[0] for row in cur.fetchall()]
        
        question_map = {}
        text_question_cols = []
        for section in form_config.get('sections', []):
            section_type = section.get('type', '')
            if section_type in ('text_questions', 'multiple_choice', 'yes_no'):
                for q in section.get('questions', []):
                    q_id = q.get('id')
                    question_map[q_id] = {
                        'text': q.get('text', ''),
                        'section_type': section_type
                    }
                    if q_id in all_cols:
                        text_question_cols.append(q_id)
        
        if not text_question_cols:
            return []
        
        text_col_sql = ', '.join(f'[{col}]' for col in text_question_cols)
        
        cur.execute(f"""
            SELECT 
                [id], 
                [participant_name], 
                [id_number],
                [submission_time],
                [course_id],
                [course_title],
                {text_col_sql}
            FROM [{table}]
            ORDER BY [submission_time] DESC
        """)
        
        responses = []
        for row in cur.fetchall():
            row_list = list(row)
            response_id = row_list[0]
            participant_name = row_list[1]
            id_number = row_list[2]
            submission_time = row_list[3]
            course_id = row_list[4]
            course_title = row_list[5]
            text_values = row_list[6:]
            participant_email = _get_participant_email(course_title, id_number, participant_name) or "N/A"
            
            text_responses = []
            for i, q_id in enumerate(text_question_cols):
                text_val = text_values[i]
                if text_val and str(text_val).strip():
                    q_info = question_map.get(q_id, {})
                    text_responses.append({
                        'question_id': q_id,
                        'question_text': q_info.get('text', ''),
                        'question_type': q_info.get('section_type', ''),
                        'response_text': str(text_val).strip()
                    })
            
            if text_responses:
                responses.append({
                    'response_id': response_id,
                    'participant_name': participant_name or '',
                    'participant_email': participant_email,
                    'id_number': id_number or '',
                    'form_id': form_id,
                    'form_title': form_config.get('title', form_id),
                    'course_id': course_id or '',
                    'course_title': course_title or '',
                    'submission_time': submission_time,
                    'text_responses': text_responses
                })
        
        conn.close()
        return responses
    except Exception as e:
        print(f"Error getting text question responses: {e}")
        return []
