from flask import Flask, render_template, request, jsonify, redirect, url_for, session, send_file
import json
import os
import re
import io
import base64
import copy
from collections import defaultdict
from datetime import datetime, timedelta
import uuid
from functools import wraps
from dotenv import load_dotenv
load_dotenv()
import db
try:
    import qrcode
    from qrcode.image.pil import PilImage
    QR_AVAILABLE = True
except ImportError:
    QR_AVAILABLE = False
    print('Warning: qrcode library not installed. Run: pip install qrcode[pil]')

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'akc-feedback-secret-key-2026')

ADMIN_ACCOUNT = os.environ.get('ADMIN_ACCOUNT', 'admin')
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'akc2026')

# Language code mapping for form IDs
# Maps full language name to short code and back
LANGUAGE_CODE_MAP = {
    'English': 'en',
    'Chinese': 'zh',
    'Thai': 'th',
    'Spanish': 'es',
    'French': 'fr',
    'German': 'de',
    'Japanese': 'ja',
    'Korean': 'ko',
}

CODE_TO_LANGUAGE = {v: k for k, v in LANGUAGE_CODE_MAP.items()}

def _language_to_code(language_name):
    """Convert full language name to code (e.g., 'English' -> 'en')"""
    return LANGUAGE_CODE_MAP.get(language_name, 'en')

def _code_to_language(code):
    """Convert language code to full name (e.g., 'en' -> 'English')"""
    return CODE_TO_LANGUAGE.get(code, 'English')

def _get_default_instructor_section(max_instructors=3):
    return {
        'id': 'B',
        'title': 'Instructors',
        'type': 'instructor_rating',
        'maxInstructors': max_instructors,
        'questions': []
    }

def _get_default_assessor_section(max_assessors=2):
    return {
        'id': 'A',
        'title': 'Assessors',
        'type': 'assessor_rating',
        'maxAssessors': max_assessors,
        'questions': []
    }

def _get_unique_section_id(sections, preferred_id):
    """Get a unique section ID, using preferred_id if available, else find alternative."""
    existing_ids = set(s.get('id') for s in sections)
    if preferred_id not in existing_ids:
        return preferred_id
    
    for i in range(ord('A'), ord('Z') + 1):
        alt_id = chr(i)
        if alt_id not in existing_ids:
            return alt_id
    return f"{preferred_id}_1"

def login_required(f):
    """Decorator to require login for admin routes"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


def api_login_required(f):
    """Decorator to require login for API routes"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return jsonify({'error': 'Authentication required'}), 401
        return f(*args, **kwargs)
    return decorated_function

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config.json')
DATA_DIR = os.path.join(BASE_DIR, 'data')

os.makedirs(DATA_DIR, exist_ok=True)

ALERTS_FILE = os.path.join(DATA_DIR, 'low_feedback_alerts.json')

_NEGATIVE_FEEDBACK_RE = re.compile(
    r'(?:'
    r'\b(?:bad|terrible|horrible|awful|dreadful|lousy|pathetic|mediocre|appalling|atrocious|abysmal|dismal|deplorable|disgraceful|shameful|dreadful)\b'
    r'|\b(?:disappointing|disappointed|disappointment|dissatisfied|unsatisfied|dissatisfaction)\b'
    r'|\b(?:unacceptable|unsatisfactory|inadequate|insufficient|substandard|subpar|inferior|deficient)\b'
    r'|\b(?:useless|pointless|worthless|ineffective|inefficient|unhelpful|futile|impractical)\b'
    r'|\b(?:waste|wasted|wasting|squandered)\b'
    r'|\b(?:unprofessional|rude|dismissive|arrogant|condescending|disrespectful|discourteous|impolite|hostile|aggressive|sarcastic)\b'
    r'|\b(?:unfair|biased|unjust|partial)\b'
    r'|\b(?:confusing|confused|unclear|incomprehensible|incoherent|ambiguous|vague)\b'
    r'|\b(?:disorganized|disorganised|unorganized|unorganised|chaotic|messy|unstructured|haphazard|scattered)\b'
    r'|\b(?:unprepared|underprepared|unqualified|incompetent|clueless|ill.?prepared)\b'
    r'|\b(?:irrelevant|outdated|obsolete|outmoded|inaccurate|incorrect|misleading)\b'
    r'|\b(?:boring|bored|monotonous|tedious|unengaging|uninteresting|uninspiring|unstimulating|repetitive|lifeless|dry)\b'
    r'|\b(?:rushed|overwhelming|erratic|inconsistent|disorderly|scattered)\b'
    r'|\b(?:dirty|unclean|unhygienic|uncomfortable|cramped|stuffy|noisy)\b'
    r'|\b(?:faulty|broken|damaged|defective|malfunctioning)\b'
    r'|\b(?:frustrated|frustrating|annoying|annoyed|irritating|irritated|exasperating|exasperated)\b'
    r'|\b(?:unenthusiastic|unresponsive|disengaged|demotivating|unsupportive|disinterested|indifferent)\b'
    r'|\b(?:lacking|lacked|lacks|incomplete|unfinished)\b'
    r'|\b(?:failed|failure|unsuccessful|fail)\b'
    r'|\b(?:unhappy|displeased|upset|miserable|angry|furious)\b'
    r'|\b(?:unpleasant|nasty|horrible|embarrassing)\b'
    r'|\b(?:delayed|behind\s+schedule)\b'
    r'|\b(?:complaint|complain|complaining|grievance)\b'
    r'|\b(?:neglected|overlooked|ignored)\b'
    r'|\b(?:poorly|badly)\b'
    # Compound negative phrases
    r'|(?:not|no)\s+(?:good|great|helpful|clear|organi[sz]ed|prepared|relevant|professional|effective|sufficient|satisfactory|adequate|engaging|interesting|useful|informative|well)\b'
    r'|not\s+up\s+to\s+(?:date|standard|scratch|par|expectation)'
    r'|out[\s-]of[\s-]date'
    r'|no\s+(?:improvement|progress|update|support|guidance|feedback|direction|structure)'
    r'|could\s+(?:be|use)\s+(?:better|improved?|clearer|more\s+\w+|enhanced)'
    r'|should\s+(?:be|have\s+been)\s+(?:better|improved?|clearer|more\s+\w+)'
    r'|need[s]?\s+(?:to\s+be\s+)?(?:improved?|better|updated?|fixed?|revised?|restructured?)'
    r'|room\s+for\s+improvement'
    r'|could\s+(?:do|be)\s+better'
    r'|waste\s+of\s+(?:time|money|resources)'
    r'|wasting\s+(?:my\s+|our\s+)?(?:time|money)'
    r'|too\s+(?:fast|slow|long|short|rushed|technical|complicated|complex|difficult|hard|easy|loud|quiet|hot|cold|strict|harsh|boring|advanced|basic|wordy|general|narrow|vague|broad)'
    r'|(?:hard|difficult)\s+to\s+(?:understand|follow|see|hear|read|concentrate|focus|keep\s+up)'
    r'|not\s+enough\s+(?:time|detail|content|practice|examples?|breaks?|information|feedback|depth|clarity|activities)'
    r'|need[s]?\s+more\s+(?:time|detail|content|practice|examples?|information|depth|clarity|activities|engagement|materials?|resources|exercises?)'
    r'|(?:did\s+not|didn[\']t|couldn[\']t|can[\']t|cannot|was\s+not|wasn[\']t)\s+(?:understand|follow|hear|learn|see|focus|engage|benefit|grasp|participate)'
    r'|would\s+not\s+recommend|cannot\s+recommend|not\s+recommend(?:ed)?'
    r'|lack\s+of\s+(?:clarity|organi[sz]ation|preparation|engagement|examples?|support|content|materials?|professionalism|structure|direction|communication|variety|depth|relevance)'
    r'|poor\s+(?:quality|performance|organi[sz]ation|facilities|equipment|materials?|content|delivery|communication|management|presentation|audio|ventilation|lighting)'
    r'|bad\s+(?:quality|experience|delivery|content|environment|facilities?|behaviour|behavior|attitude|examples?|management)'
    r'|very\s+(?:bad|poor|disappointing|confusing|unclear|boring|slow|fast|harsh|strict|difficult|loud|noisy|hot|cold)'
    r'|extremely\s+(?:bad|poor|disappointing|confusing|unclear|boring|difficult|slow|fast)'
    r'|so\s+(?:bad|poor|boring|confusing|slow|fast|difficult|long|short)'
    r'|need[s]?\s+(?:a\s+lot\s+of\s+)?improvement'
    r'|not\s+satisfied|not\s+happy|not\s+pleased'
    r'|more\s+(?:preparation|practice|examples?|time|clarity|structure|engagement)\s+(?:is|are|was|were)?\s*needed'
    r')',
    re.IGNORECASE
)

def _extract_negative_matches(text):
    """Return deduplicated list of matched negative phrases found in text."""
    seen = set()
    results = []
    for m in _NEGATIVE_FEEDBACK_RE.finditer(text or ''):
        word = m.group(0).strip()
        key  = word.lower()
        if key not in seen:
            seen.add(key)
            results.append(word)
    return results


def load_alerts():
    """Load low feedback alerts from JSON file"""
    if os.path.exists(ALERTS_FILE):
        with open(ALERTS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []


def save_alerts_data(alerts):
    """Save low feedback alerts to JSON file"""
    with open(ALERTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(alerts, f, indent=2, ensure_ascii=False)


def save_low_feedback_alerts(form_id, course_id, course, data, form_config):
    """Detect low ratings (≤ 2) and negative sentiment in free-text responses,
    creating alert records for admin review.
    """
    alerts = load_alerts()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    rating_labels = {1: 'Poor', 2: 'Unsatisfactory'}

    RATING_SECTION_TYPES = {'rating', 'instructor_rating', 'assessor_rating'}
    rating_q_ids = set()
    text_q_ids   = set()
    text_q_map   = {} 
    q_texts      = {}  
    for section in form_config.get('sections', []):
        s_type = section.get('type', '')
        for q in section.get('questions', []):
            q_texts[q['id']] = q['text']
            if s_type in RATING_SECTION_TYPES:
                rating_q_ids.add(q['id'])
            elif s_type == 'text_questions':
                text_q_ids.add(q['id'])
                text_q_map[q['id']] = q['text']

    new_rating_alert_q_ids = set()
    for key, value in data.items():
        if key.endswith('_comment'):
            continue

        base_key = key
        if '_' in key:
            prefix, rest = key.split('_', 1)
            if prefix and prefix[0] in ('B', 'A') and prefix[1:].isdigit():
                base_key = rest

        if base_key in text_q_ids:
            continue
        if base_key not in rating_q_ids:
            continue
        try:
            rating = int(value)
        except (ValueError, TypeError):
            continue

        if rating <= 2:
            q_text  = q_texts.get(base_key, q_texts.get(key, key))
            comment = data.get(f'{key}_comment', '')
            alerts.append({
                'id': str(uuid.uuid4())[:12],
                'course_id': course_id,
                'course_title': course.get('course_title', ''),
                'course_date': course.get('course_date', ''),
                'form_id': form_id,
                'participant_name': data.get('name', 'Anonymous'),
                'question_id': key,
                'question_text': q_text,
                'rating': rating,
                'rating_label': rating_labels.get(rating, str(rating)),
                'comment': comment,
                'alert_type': 'rating',
                'matched_keywords': '',
                'status': 'new',
                'action_notes': '',
                'submitted_at': now,
                'updated_at': '',
            })
            new_rating_alert_q_ids.add(base_key)

    for qid, q_text in text_q_map.items():
        response_val = str(data.get(qid, '')).strip()
        if len(response_val) < 2:
            continue
        matches = _extract_negative_matches(response_val)
        matched_keywords = ', '.join(dict.fromkeys(m.lower() for m in matches))[:300] if matches else ''
        
        alerts.append({
            'id': str(uuid.uuid4())[:12],
            'course_id': course_id,
            'course_title': course.get('course_title', ''),
            'course_date': course.get('course_date', ''),
            'form_id': form_id,
            'participant_name': data.get('name', 'Anonymous'),
            'question_id': qid,
            'question_text': q_text,
            'rating': None,
            'rating_label': '',
            'comment': response_val,
            'alert_type': 'text_feedback',
            'matched_keywords': matched_keywords,
            'status': 'new',
            'action_notes': '',
            'submitted_at': now,
            'updated_at': '',
        })

    for key, value in data.items():
        if not key.endswith('_comment'):
            continue
        comment_val = str(value).strip() if value else ''
        if len(comment_val) < 2:
            continue
        base_q_id = key[:-len('_comment')]
        if base_q_id in new_rating_alert_q_ids:
            continue 
        matches = _extract_negative_matches(comment_val)
        matched_keywords = ', '.join(dict.fromkeys(m.lower() for m in matches))[:300] if matches else ''
        
        q_text = q_texts.get(base_q_id, base_q_id)
        alerts.append({
            'id': str(uuid.uuid4())[:12],
            'course_id': course_id,
            'course_title': course.get('course_title', ''),
            'course_date': course.get('course_date', ''),
            'form_id': form_id,
            'participant_name': data.get('name', 'Anonymous'),
            'question_id': base_q_id,
            'question_text': q_text,
            'rating': None,
            'rating_label': '',
            'comment': comment_val,
            'alert_type': 'rating_comment',
            'matched_keywords': matched_keywords,
            'status': 'new',
            'action_notes': '',
            'submitted_at': now,
            'updated_at': '',
        })

    save_alerts_data(alerts)

def _norm_name(name):
    """Normalize a name for comparison: collapse whitespace, uppercase"""
    return ' '.join(name.strip().split()).upper()

def _norm_id(id_number):
    """Normalize an identification number for comparison: strip, uppercase"""
    return id_number.strip().upper()

def has_submitted(course_id, identifier):
    """Check if a student has already submitted for this course."""
    course = db.get_course_by_id(course_id)
    form_id = (course or {}).get('form_id', '')
    config = load_config()
    form_config = config['forms'].get(form_id, {})
    form_title = form_config.get('title', form_id)
    return db.has_submitted_db(course_id, identifier, form_title)

def load_config():
    """Load app config from JSON and forms from FBS_Forms."""
    base = {}
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                base = json.load(f)
        except Exception:
            base = {}

    if not isinstance(base, dict):
        base = {}

    base['courses'] = base.get('courses', [])
    base['forms'] = db.get_active_forms_map()
    return base

def save_config(config):
    local_cfg = {
        'courses': config.get('courses', [])
    }
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(local_cfg, f, indent=2, ensure_ascii=False)


def sync_forms_registry_with_config(config):
    """Upsert non-archived forms from config into FBS_Forms."""
    results = {}
    for form_id, form in config.get('forms', {}).items():
        if form.get('is_archived'):
            continue
        ok = db.register_form(
            form_id,
            form.get('title', form_id),
            form.get('formNumber', ''),
            form.get('description', ''),
            form,
        )
        results[form_id] = ok
    all_ok = all(results.values()) if results else True
    return all_ok, results

def _slugify_form_id(title):
    s = title.strip().lower()
    s = re.sub(r'[^a-z0-9]+', '_', s)
    return s.strip('_') or 'form_' + uuid.uuid4().hex[:8]

def save_response(form_id, course_id, data, id_number='', language='English'):
    """Save form response to AKC_FBS database."""
    course = db.get_course_by_id(course_id)
    config = load_config()
    form_config = config['forms'].get(form_id, {})
    form_title  = form_config.get('title', form_id)
    participant_name = data.get('name', '')
    position         = data.get('position', '')

    table_ok, table_msg = db.create_form_response_table(form_title, form_config)
    if not table_ok:
        print(f"ERROR: Could not create response table [{form_title}]: {table_msg}")
        return False
    
    ok = db.save_response_to_db(
        form_id, course_id, course or {},
        participant_name, id_number, position, data, form_title, form_config, language)
    if not ok:
        print(f"ERROR: Could not save response to database for form_id={form_id}")
        return False
    
    print(f"SUCCESS: Response saved to table [{form_title}] for participant {participant_name} (ID: {id_number}) in language {language}")
    return True


def _extract_language_from_form_id(form_id):
    """Extract language from form config using form_id.
    
    Returns the language as stored in form config, or 'English' as default.
    """
    config = load_config()
    form_config = config['forms'].get(form_id, {})
    return form_config.get('language', 'English')


@app.route('/')
def index():
    """Redirect to admin page"""
    return redirect(url_for('admin'))


@app.route('/scan')
def scan_page():
    """Universal scan page."""
    return render_template('scan.html')


@app.route('/api/scan/lookup', methods=['POST'])
def scan_lookup():
    data = request.json or {}
    class_code = (data.get('class_code') or '').strip()
    id_number  = (data.get('id_number')  or '').strip()

    if not class_code or not id_number:
        return jsonify({'error': 'Class code and identification number are required.'}), 400

    participant_name = db.get_participant_name_by_id(class_code, id_number)
    if not participant_name:
        return jsonify({'error': 'Identification number not found for that class code. Please check and try again.'}), 404

    matches = db.get_active_courses_by_title(class_code)

    if not matches:
        return jsonify({'error': 'No active QR session found for that class code. Please ask your instructor.'}), 404

    if len(matches) == 1:
        course = matches[0]
        if has_submitted(course['id'], id_number):
            return jsonify({'error': 'You have already submitted feedback for this class. Thank you!'}), 400
        session['student_name'] = participant_name
        session['student_id_number'] = id_number.upper()
        session['student_course_id'] = course['id']
        return jsonify({'redirect': url_for('form_page', course_id=course['id'])})

    options = []
    for c in matches:
        form_label = {
            'form1': 'Trainer Evaluation',
            'form2': 'Assessor Evaluation'
        }.get(c['form_id'], c['form_id'])
        already = has_submitted(c['id'], id_number)
        options.append({
            'course_id': c['id'],
            'form_label': form_label,
            'course_date': c.get('course_date', ''),
            'already_submitted': already
        })
    return jsonify({'options': options, 'participant_name': participant_name,
                    'class_code': class_code, 'id_number': id_number.upper()})

@app.route('/api/scan/select', methods=['POST'])
def scan_select():
    """After user picks one option from multiple sessions."""
    data = request.json or {}
    course_id   = (data.get('course_id')   or '').strip()
    id_number   = (data.get('id_number')   or '').strip()
    participant_name = (data.get('participant_name') or '').strip()

    if not course_id or not id_number:
        return jsonify({'error': 'Missing parameters.'}), 400

    course = db.get_course_by_id(course_id)
    if not course:
        return jsonify({'error': 'Session not found.'}), 404

    if has_submitted(course_id, id_number):
        return jsonify({'error': 'You have already submitted feedback for this session. Thank you!'}), 400

    session['student_name'] = participant_name
    session['student_id_number'] = id_number.upper()
    session['student_course_id'] = course_id
    return jsonify({'redirect': url_for('form_page', course_id=course_id)})

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Admin login page"""
    if session.get('logged_in'):
        return redirect(url_for('admin'))
    
    error = None
    if request.method == 'POST':
        account = request.form.get('account', '')
        password = request.form.get('password', '')
        
        if account == ADMIN_ACCOUNT and password == ADMIN_PASSWORD:
            session['logged_in'] = True
            session['admin_account'] = account
            return redirect(url_for('admin'))
        else:
            error = 'Invalid account number or password'
    
    return render_template('admin_login.html', error=error)

@app.route('/logout')
def logout():
    """Admin logout"""
    session.clear()
    return redirect(url_for('login'))

@app.route('/admin')
@login_required
def admin():
    """Admin dashboard"""
    config = load_config()
    def _detect_personnel(sections):
        types = {s.get('type') for s in sections}
        if 'assessor_rating' in types:
            return 'assessor'
        if 'instructor_rating' in types:
            return 'instructor'
        return 'none'
    form_personnel = {
        fid: _detect_personnel(f.get('sections', []))
        for fid, f in config['forms'].items()
    }
    form_qr_fields = {
        fid: f['qr_fields']
        for fid, f in config['forms'].items()
        if 'qr_fields' in f
    }
    return render_template('admin.html', config=config, form_personnel=form_personnel, form_qr_fields=form_qr_fields)

@app.route('/admin/form/<form_id>')
@login_required
def admin_form(form_id):
    """Admin page for editing a specific form"""
    config = load_config()
    form = config['forms'].get(form_id)
    if not form:
        return "Form not found", 404
    return render_template('admin_form.html', form=form, config=config)

@app.route('/api/forms/<form_id>', methods=['GET'])
@api_login_required
def get_form(form_id):
    """Get form configuration"""
    config = load_config()
    form = config['forms'].get(form_id)
    if not form:
        return jsonify({'error': 'Form not found'}), 404
    return jsonify(form)

@app.route('/api/forms/<form_id>', methods=['PUT'])
@api_login_required
def update_form(form_id):
    """Update form configuration"""
    config = load_config()
    if form_id not in config['forms']:
        return jsonify({'error': 'Form not found'}), 404
    
    data = request.json

    form_ok = db.register_form(
        form_id,
        data.get('title', form_id),
        data.get('formNumber', ''),
        data.get('description', ''),
        data,
    )
    if not form_ok:
        return jsonify({'error': 'Could not save form to FBS_Forms'}), 500

    form_title = data.get('title', form_id)
    ok, msg = db.sync_form_response_table(form_title, data)
    if not ok:
        return jsonify({'error': f'Form saved, but response table sync failed: {msg}'}), 500
    return jsonify({'success': True, 'db_sync': msg})

@app.route('/api/forms/<form_id>/sections', methods=['POST'])
@api_login_required
def add_section(form_id):
    """Add a new section to form"""
    config = load_config()
    if form_id not in config['forms']:
        return jsonify({'error': 'Form not found'}), 404
    
    data = request.json
    config['forms'][form_id]['sections'].append(data)

    form = config['forms'][form_id]
    ok = db.register_form(
        form_id,
        form.get('title', form_id),
        form.get('formNumber', ''),
        form.get('description', ''),
        form,
    )
    if not ok:
        return jsonify({'error': 'Could not save form changes to database'}), 500

    table_ok, table_msg = db.sync_form_response_table(form.get('title', form_id), form)
    return jsonify({'success': True, 'db_sync': table_msg if table_ok else 'table sync failed'})

@app.route('/api/forms/<form_id>/sections/<section_id>/questions', methods=['POST'])
@api_login_required
def add_question(form_id, section_id):
    """Add a question to a section"""
    config = load_config()
    if form_id not in config['forms']:
        return jsonify({'error': 'Form not found'}), 404
    
    data = request.json
    form = config['forms'][form_id]
    
    for section in form['sections']:
        if section['id'] == section_id:
            section['questions'].append(data)
            break

    ok = db.register_form(
        form_id,
        form.get('title', form_id),
        form.get('formNumber', ''),
        form.get('description', ''),
        form,
    )
    if not ok:
        return jsonify({'error': 'Could not save form changes to database'}), 500

    table_ok, table_msg = db.sync_form_response_table(form.get('title', form_id), form)
    return jsonify({'success': True, 'db_sync': table_msg if table_ok else 'table sync failed'})

@app.route('/api/forms/<form_id>/sections/<section_id>/questions/<question_id>', methods=['DELETE'])
@api_login_required
def delete_question(form_id, section_id, question_id):
    """Delete a question from a section"""
    config = load_config()
    if form_id not in config['forms']:
        return jsonify({'error': 'Form not found'}), 404
    
    form = config['forms'][form_id]
    
    for section in form['sections']:
        if section['id'] == section_id:
            section['questions'] = [q for q in section['questions'] if q['id'] != question_id]
            break

    ok = db.register_form(
        form_id,
        form.get('title', form_id),
        form.get('formNumber', ''),
        form.get('description', ''),
        form,
    )
    if not ok:
        return jsonify({'error': 'Could not save form changes to database'}), 500

    table_ok, table_msg = db.sync_form_response_table(form.get('title', form_id), form)
    return jsonify({'success': True, 'db_sync': table_msg if table_ok else 'table sync failed'})

@app.route('/api/db/test', methods=['GET'])
@api_login_required
def test_db_connection():
    """Test database connection"""
    success, message = db.test_connection()
    return jsonify({'success': success, 'message': message})

@app.route('/api/db/courses', methods=['GET'])
@api_login_required
def search_db_courses():
    """Search courses from database"""
    search_term = request.args.get('search', '')
    limit = request.args.get('limit', 50, type=int)
    courses = db.get_courses_from_db(search_term, limit)
    return jsonify(courses)

@app.route('/api/db/participants', methods=['GET'])
@api_login_required
def get_participants():
    """Get participants for a class with pagination."""
    class_code = request.args.get('class_code', '')
    offset = request.args.get('offset', 0, type=int)
    limit = request.args.get('limit', 20, type=int)

    if not class_code:
        return jsonify({'error': 'Class code required'}), 400

    result = db.get_participants_by_class(class_code, offset, limit)
    if 'error' in result:
        return jsonify(result)

    config = load_config()
    class_code_norm = class_code.strip().upper()
    matching_courses = db.get_courses_by_title(class_code_norm)
    matching_course_ids = [c['id'] for c in matching_courses]
    form_titles = list({
        config['forms'].get(c.get('form_id', ''), {}).get('title', c.get('form_id', ''))
        for c in matching_courses
    })

    submitted_ids = db.get_submitted_ids_for_courses(matching_course_ids, form_titles)

    for p in result.get('participants', []):
        id_norm = _norm_id(p.get('id_number', ''))
        p['form_submitted'] = bool(id_norm) and id_norm in submitted_ids

    return jsonify(result)

@app.route('/api/db/update-survey-sent', methods=['POST'])
@api_login_required
def update_survey_sent():
    """Update Survey Sent flag for a participant"""
    data = request.json
    course_code = data.get('course_code')
    participant_name = data.get('participant_name')
    
    if not course_code or not participant_name:
        return jsonify({'error': 'Course code and participant name required'}), 400
    
    success = db.update_survey_sent(course_code, participant_name, True)
    return jsonify({'success': success})

@app.route('/api/db/create-tables', methods=['POST'])
@api_login_required
def create_tables():
    """Ensure FBS_Forms and per-form response tables exist in AKC_FBS."""
    config = load_config()
    ok_base, msg_base = db.init_fbs_tables()
    results = {'FBS_Forms': msg_base}
    
    ok_rect, msg_rect = db.init_rectification_log_table()
    results['FBS_Rectification_Log'] = msg_rect

    forms_ok, forms_sync = sync_forms_registry_with_config(config)
    synced_count = sum(1 for ok in forms_sync.values() if ok)
    results['FBS_Forms_sync'] = f"Synced {synced_count}/{len(forms_sync)} forms"

    for form_id, form in config['forms'].items():
        if not form.get('is_archived'):
            ok, msg = db.create_form_response_table(form.get('title', form_id), form)
            results[form_id] = msg
    all_ok = ok_base and forms_ok and all('Error' not in m for m in results.values())
    return jsonify({'success': all_ok, 'message': ' | '.join(f"{k}: {v}" for k, v in results.items())})


@app.route('/api/forms/<form_id>/create-table', methods=['POST'])
@api_login_required
def create_form_table(form_id):
    """Create the per-form response table for the given form."""
    config = load_config()
    form = config['forms'].get(form_id)
    if not form:
        return jsonify({'error': 'Form not found'}), 404
    form_title = form.get('title', form_id)
    ok, message = db.create_form_response_table(form_title, form)
    return jsonify({'success': ok, 'message': message,
                    'table': db._get_table_name(form_title)})

@app.route('/api/forms/<form_id>/table-status', methods=['GET'])
@api_login_required
def form_table_status(form_id):
    """Return whether the per-form response table already exists."""
    config = load_config()
    form = config['forms'].get(form_id)
    if not form:
        return jsonify({'error': 'Form not found'}), 404
    form_title = form.get('title', form_id)
    table_name = db._get_table_name(form_title)
    exists = db.form_table_exists(form_title)
    return jsonify({'exists': exists, 'table': table_name})

@app.route('/api/db/course-dates', methods=['GET'])
@api_login_required
def get_course_dates():
    """Get all available registration dates from database"""
    dates = db.get_course_dates()
    return jsonify(dates)

@app.route('/api/db/class-codes', methods=['GET'])
@api_login_required
def get_class_codes():
    """Get class codes for a specific registration date"""
    date = request.args.get('date', '')
    if not date:
        return jsonify({'error': 'Date required'}), 400
    codes = db.get_class_codes_by_date(date)
    return jsonify(codes)

@app.route('/api/courses', methods=['GET'])
@api_login_required
def get_courses():
    """Get all course sessions from the database."""
    return jsonify(db.get_all_courses_from_db())

@app.route('/api/courses', methods=['POST'])
@api_login_required
def create_course():
    """Create a new course and generate QR code"""
    config = load_config()
    data = request.json
    course = {
        'form_id': data['form_id'],
        'course_title': data['course_title'],
        'course_date': data['course_date'],
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    _form_cfg = config['forms'].get(data['form_id'], {})
    _qr_fields = _form_cfg.get('qr_fields')
    _section_types = {s.get('type') for s in _form_cfg.get('sections', [])}

    if _qr_fields:
        if _qr_fields.get('classroom', {}).get('show'):
            course['classroom'] = data.get('classroom', '')
        if _qr_fields.get('assessment_location', {}).get('show'):
            course['assessment_location'] = data.get('assessment_location', '')
        if _qr_fields.get('instructors', {}).get('show'):
            num = data.get('num_instructors', 1)
            course['num_instructors'] = num
            course['instructors'] = data.get('instructors', [])
        else:
            course['num_instructors'] = 0
            course['instructors'] = []
        if _qr_fields.get('assessors', {}).get('show'):
            num = data.get('num_assessors', 1)
            course['num_assessors'] = num
            course['assessors'] = data.get('assessors', [])
        else:
            course['num_assessors'] = 0
            course['assessors'] = []

        custom_vals = data.get('custom_field_values', {})
        for cf in _qr_fields.get('custom_fields', []):
            course[cf['id']] = custom_vals.get(cf['id'], '')
    elif 'assessor_rating' in _section_types:
        course['assessment_location'] = data.get('assessment_location', '')
        course['num_assessors'] = data.get('num_assessors', 1)
        course['assessors'] = data.get('assessors', [])
    elif 'instructor_rating' in _section_types:
        course['classroom'] = data.get('classroom', '')
        course['num_instructors'] = data.get('num_instructors', 1)
        course['instructors'] = data.get('instructors', [])
    else:
        course['classroom'] = data.get('classroom', '')
        course['num_instructors'] = 0
        course['instructors'] = []

    inserted = False
    for _ in range(5):
        course['id'] = uuid.uuid4().hex[:20]
        if db.create_course_in_db(course):
            inserted = True
            break
    if not inserted:
        return jsonify({'success': False, 'error': 'Could not create course session in database. Please try again.'}), 500

    # Generate QR code
    qr_data = None
    if QR_AVAILABLE:
        public_base = os.environ.get('PUBLIC_URL', '').rstrip('/') or request.host_url.rstrip('/')
        form_url = public_base + f'/student-login/{course["id"]}'
        qr = qrcode.QRCode(version=1, box_size=10, border=4)
        qr.add_data(form_url)
        qr.make(fit=True)
        img = qr.make_image(fill_color='black', back_color='white')
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        buf.seek(0)
        qr_data = base64.b64encode(buf.read()).decode('utf-8')

    return jsonify({'success': True, 'course': course, 'qr_code': qr_data})

@app.route('/api/forms/by-base/<base_form_id>', methods=['GET'])
@api_login_required
def get_form_languages(base_form_id):
    """Get all language versions of a base form.
    
    Returns array of forms with same base_form_id, grouped by language.
    """
    config = load_config()
    form_versions = []
    
    for form_id, form_config in config['forms'].items():
        if form_config.get('base_form_id') == base_form_id and not form_config.get('is_archived'):
            form_versions.append({
                'form_id': form_id,
                'title': form_config.get('title', ''),
                'language': form_config.get('language', 'English'),
            })
    
    return jsonify({'base_form_id': base_form_id, 'versions': form_versions})

@app.route('/api/courses/<course_id>', methods=['DELETE'])
@api_login_required
def close_course(course_id):
    """Close a course QR session. The record is kept in the database."""
    ok = db.deactivate_course(course_id)
    return jsonify({'success': ok})


@app.route('/api/courses/<course_id>/reactivate', methods=['PATCH'])
@api_login_required
def reactivate_course(course_id):
    """Re-open a previously closed QR session."""
    ok = db.reactivate_course(course_id)
    return jsonify({'success': ok})

@app.route('/api/courses/<course_id>/qrcode', methods=['GET'])
@api_login_required
def get_course_qrcode(course_id):
    """Get QR code image for a course"""
    course = db.get_course_by_id(course_id)
    if not course:
        return jsonify({'error': 'Course not found'}), 404
    if not QR_AVAILABLE:
        return jsonify({'error': 'QR code library not installed'}), 500
    public_base = os.environ.get('PUBLIC_URL', '').rstrip('/') or request.host_url.rstrip('/')
    form_url = public_base + f'/student-login/{course_id}'
    qr = qrcode.QRCode(version=1, box_size=10, border=4)
    qr.add_data(form_url)
    qr.make(fit=True)
    img = qr.make_image(fill_color='black', back_color='white')
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    buf.seek(0)
    return send_file(buf, mimetype='image/png', as_attachment=True,
                     download_name=f'QR_{course["course_title"]}_{course["course_date"]}.png')

@app.route('/api/scan/qrcode', methods=['GET'])
@api_login_required
def get_universal_qrcode():
    if not QR_AVAILABLE:
        return jsonify({'error': 'QR code library not installed'}), 500
    public_base = os.environ.get('PUBLIC_URL', '').rstrip('/') or request.host_url.rstrip('/')
    scan_url = public_base + '/scan'
    qr = qrcode.QRCode(version=1, box_size=10, border=4)
    qr.add_data(scan_url)
    qr.make(fit=True)
    img = qr.make_image(fill_color='black', back_color='white')
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    buf.seek(0)
    return send_file(buf, mimetype='image/png', as_attachment=True,
                     download_name='AKC_Universal_QR.png')

@app.route('/student-login/<course_id>', methods=['GET', 'POST'])
def student_login(course_id):
    """Student login page - verifies the student by identification number"""
    course = db.get_course_by_id(course_id)
    if not course:
        return "Course not found. The QR code may be invalid.", 404
    if not course.get('is_active'):
        return "This QR session has been closed by the administrator.", 403

    error = None
    if request.method == 'POST':
        id_number = request.form.get('id_number', '').strip()
        if not id_number:
            error = 'Please enter your Identification Number.'
        elif has_submitted(course_id, id_number):
            error = 'You have already submitted feedback for this class. Thank you!'
        else:
            participant_name = db.get_participant_name_by_id(
                course['course_title'], id_number
            )
            if participant_name:
                session['student_name'] = participant_name
                session['student_id_number'] = id_number.upper()
                session['student_course_id'] = course_id
                return redirect(url_for('form_page', course_id=course_id))
            else:
                error = 'Your Identification Number was not found in the participant list for this class. Please check and try again.'
    return render_template('student_login.html', course=course, error=error)

@app.route('/form/<course_id>')
def form_page(course_id):
    """Public form page for participants to fill"""
    # Students must pass through the login page first
    if session.get('student_course_id') != course_id:
        return redirect(url_for('student_login', course_id=course_id))

    course = db.get_course_by_id(course_id)
    if not course:
        return "Course not found. The link may be invalid.", 404
    if not course.get('is_active'):
        return "This QR session has been closed by the administrator.", 403

    config = load_config()
    form = config['forms'].get(course['form_id'])
    if not form:
        return "Form not found", 404
    
    student_name = session.get('student_name', '')
    return render_template('form.html', form=form, course=course, student_name=student_name)

@app.route('/api/submit/<course_id>', methods=['POST'])
def submit_form(course_id):
    """Submit form response"""
    if session.get('student_course_id') != course_id:
        return jsonify({'error': 'Unauthorized. Please scan the QR code and log in first.'}), 401

    student_name = session.get('student_name', '')
    student_id = session.get('student_id_number', '')

    if has_submitted(course_id, student_id or student_name):
        return jsonify({'error': 'You have already submitted feedback for this class.'}), 400

    course = db.get_course_by_id(course_id)
    if not course:
        return jsonify({'error': 'Course not found'}), 404

    data = request.json
    language = _extract_language_from_form_id(course['form_id'])
    success = save_response(course['form_id'], course_id, data, student_id, language)
    if not success:
        print(f"ABORT: Form submission failed for {student_name} (ID: {student_id})")
        session.pop('student_name', None)
        session.pop('student_id_number', None)
        session.pop('student_course_id', None)
        return jsonify({'error': 'Failed to save your feedback. Please try again.'}), 500
    
    config = load_config()
    form_config = config['forms'].get(course['form_id'], {})
    try:
        save_low_feedback_alerts(course['form_id'], course_id, course, data, form_config)
        print(f"SUCCESS: Low feedback alerts saved for {student_name}")
    except Exception as e:
        print(f"Warning: Could not save low feedback alerts: {e}")

    session.pop('student_name', None)
    session.pop('student_id_number', None)
    session.pop('student_course_id', None)

    return jsonify({'success': True, 'message': 'Thank you for your feedback!'})

@app.route('/api/alerts', methods=['GET'])
@api_login_required
def get_alerts():
    """Get low feedback alerts, optionally filtered by status and/or alert type."""
    alerts = load_alerts()
    status_filter = request.args.get('status', '')
    type_filter   = request.args.get('alert_type', '')
    if status_filter:
        alerts = [a for a in alerts if a.get('status') == status_filter]
    if type_filter:
        alerts = [a for a in alerts if a.get('alert_type', 'rating') == type_filter]
    alerts.sort(key=lambda a: a.get('submitted_at', ''), reverse=True)
    return jsonify(alerts)

@app.route('/api/alerts/summary', methods=['GET'])
@api_login_required
def get_alerts_summary():
    """Get alert counts grouped by status"""
    alerts = load_alerts()
    summary = {'new': 0, 'acknowledged': 0, 'in_progress': 0, 'resolved': 0, 'total': len(alerts)}
    for a in alerts:
        status = a.get('status', 'new')
        if status in summary:
            summary[status] += 1
    return jsonify(summary)

@app.route('/api/alerts/<alert_id>', methods=['PUT'])
@api_login_required
def update_alert(alert_id):
    """Update alert status and/or action notes"""
    alerts = load_alerts()
    data = request.json
    for alert in alerts:
        if alert['id'] == alert_id:
            if 'status' in data:
                alert['status'] = data['status']
            if 'action_notes' in data:
                alert['action_notes'] = data['action_notes']
            alert['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            break
    else:
        return jsonify({'error': 'Alert not found'}), 404
    save_alerts_data(alerts)
    return jsonify({'success': True})


@app.route('/api/alerts/analysis', methods=['GET'])
@api_login_required
def get_alerts_analysis():
    """Group unresolved alerts by question and return priority-ranked hotspots."""
    alerts = load_alerts()
    active = [a for a in alerts if a.get('status') != 'resolved']

    groups = defaultdict(lambda: {
        'question_id': '', 'question_text': '', 'forms': set(),
        'ratings': [], 'comments': [], 'alert_ids': [], 'count': 0
    })
    for a in active:
        qid = a.get('question_id', '')
        g = groups[qid]
        g['question_id'] = qid
        g['question_text'] = g['question_text'] or a.get('question_text', qid)
        g['forms'].add(a.get('form_id', ''))
        g['ratings'].append(a.get('rating', 0))
        if a.get('comment'):
            g['comments'].append(a['comment'])
        g['alert_ids'].append(a.get('id'))
        g['count'] += 1

    result = []
    for qid, g in groups.items():
        ratings = g['ratings']
        avg = sum(ratings) / len(ratings) if ratings else 0
        priority = round(g['count'] * (3 - avg), 2)
        result.append({
            'question_id': g['question_id'],
            'question_text': g['question_text'],
            'forms': list(g['forms']),
            'count': g['count'],
            'avg_rating': round(avg, 2),
            'priority_score': priority,
            'alert_ids': g['alert_ids'],
            'comments': g['comments'][:5]
        })
    result.sort(key=lambda x: x['priority_score'], reverse=True)
    return jsonify(result)

@app.route('/api/alerts/batch', methods=['PUT'])
@api_login_required
def batch_update_alerts():
    """Update multiple alerts at once by their IDs."""
    data = request.json
    ids = set(data.get('ids', []))
    new_status = data.get('status', '')
    action_notes = data.get('action_notes', '')
    if not ids or not new_status:
        return jsonify({'error': 'ids and status are required'}), 400
    alerts = load_alerts()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    updated = 0
    for a in alerts:
        if a.get('id') in ids:
            a['status'] = new_status
            if action_notes:
                a['action_notes'] = action_notes
            a['updated_at'] = now
            updated += 1
    save_alerts_data(alerts)
    return jsonify({'success': True, 'updated': updated})

@app.route('/api/alerts/<alert_id>', methods=['DELETE'])
@api_login_required
def delete_alert(alert_id):
    """Delete a single alert by ID"""
    alerts = load_alerts()
    original_len = len(alerts)
    alerts = [a for a in alerts if a.get('id') != alert_id]
    if len(alerts) == original_len:
        return jsonify({'error': 'Alert not found'}), 404
    save_alerts_data(alerts)
    return jsonify({'success': True})


@app.route('/api/alerts/batch', methods=['DELETE'])
@api_login_required
def batch_delete_alerts():
    data = request.json or {}
    ids = set(data.get('ids', []))
    status_filter = data.get('status_filter', '')
    alerts = load_alerts()
    before = len(alerts)
    if ids:
        alerts = [a for a in alerts if a.get('id') not in ids]
    elif status_filter:
        alerts = [a for a in alerts if a.get('status') != status_filter]
    else:
        return jsonify({'error': 'Provide ids or status_filter'}), 400
    deleted = before - len(alerts)
    save_alerts_data(alerts)
    return jsonify({'success': True, 'deleted': deleted})

@app.route('/api/analysis/summary', methods=['GET'])
@api_login_required
def get_analysis_summary():
    """Return high-level collection stats: total responses per form + alert counts."""
    config = load_config()
    counts = db.get_response_count_by_form(config['forms'])
    result = {}
    for form_id, form in config['forms'].items():
        result[form_id] = {
            'title': form.get('title', form_id),
            'total_responses': counts.get(form_id, 0)
        }
    alerts = load_alerts()
    result['_alerts'] = {
        'total': len(alerts),
        'unresolved': sum(1 for a in alerts if a.get('status') != 'resolved')
    }
    return jsonify(result)

@app.route('/api/analysis/ratings', methods=['GET'])
@api_login_required
def get_analysis_ratings():
    """Compute per-question averages and rating distributions from the database."""
    form_id = request.args.get('form_id', 'form1')
    date_from_str = request.args.get('date_from', '').strip()
    date_to_str = request.args.get('date_to', '').strip()
    course_filter = request.args.get('course', '').strip()

    config = load_config()
    form = config['forms'].get(form_id)
    if not form:
        return jsonify({'error': 'Form not found'}), 404

    try:
        dt_from = datetime.strptime(date_from_str, '%Y-%m-%d') if date_from_str else None
    except ValueError:
        dt_from = None
    try:
        dt_to = datetime.strptime(date_to_str, '%Y-%m-%d') if date_to_str else None
    except ValueError:
        dt_to = None

    rating_q_map = {}
    text_q_map = {}
    for section in form.get('sections', []):
        s_type = section.get('type', '')
        questions = section.get('questions', [])
        if s_type == 'instructor_rating':
            max_inst = section.get('maxInstructors', 3)
            for i in range(1, max_inst + 1):
                for q in questions:
                    col = f"B{i}_{q['id']}"
                    rating_q_map[col] = f"Instructor {i}: {q.get('text', q['id'])}"
        elif s_type == 'assessor_rating':
            max_assess = section.get('maxAssessors', 2)
            for i in range(1, max_assess + 1):
                for q in questions:
                    col = f"A{i}_{q['id']}"
                    rating_q_map[col] = f"Assessor {i}: {q.get('text', q['id'])}"
        elif s_type == 'rating':
            for q in questions:
                rating_q_map[q['id']] = q.get('text', q['id'])
        elif s_type == 'text_questions':
            for q in questions:
                text_q_map[q['id']] = q.get('text', q['id'])

    rows = db.get_responses_for_analysis(form_id, form, dt_from, dt_to, course_filter)

    if not rows:
        courses_available = db.get_distinct_courses_for_form(form_id, form)
        return jsonify({'questions': [], 'text_responses': [], 'total_rows': 0,
                        'filtered_rows': 0, 'courses_available': courses_available})

    stats = {}
    text_agg = {}
    for row in rows:
        answers = row.get('answers', {})
        for qid, qtext in rating_q_map.items():
            val = answers.get(qid)
            try:
                r = int(float(str(val))) if val is not None else None
            except (ValueError, TypeError):
                continue
            if r is None or r < 1 or r > 5:
                continue
            if qid not in stats:
                stats[qid] = {'text': qtext, 'count': 0, 'total': 0, 'dist': {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}}
            stats[qid]['count'] += 1
            stats[qid]['total'] += r
            stats[qid]['dist'][r] += 1
        for qid, qtext in text_q_map.items():
            val = str(answers.get(qid, '') or '').strip()
            if val and val.lower() not in ('none', '-', ''):
                if qid not in text_agg:
                    text_agg[qid] = {'text': qtext, 'responses': []}
                text_agg[qid]['responses'].append(val)

    questions_out = []
    for qid, s in stats.items():
        avg = round(s['total'] / s['count'], 2) if s['count'] else 0
        questions_out.append({'id': qid, 'text': s['text'], 'count': s['count'],
                              'avg': avg, 'dist': s['dist']})
    questions_out.sort(key=lambda x: x['avg'])

    courses_available = db.get_distinct_courses_for_form(form_id, form)
    return jsonify({
        'questions': questions_out,
        'text_responses': list(text_agg.values()),
        'total_rows': len(rows),
        'filtered_rows': len(rows),
        'courses_available': courses_available
    })

@app.route('/api/analysis/text', methods=['GET'])
@api_login_required
def get_analysis_text():
    form_id = request.args.get('form_id', 'form1')
    date_from_str = request.args.get('date_from', '').strip()
    date_to_str   = request.args.get('date_to', '').strip()
    course_filter = request.args.get('course', '').strip()

    config = load_config()
    form = config['forms'].get(form_id)
    if not form:
        return jsonify({'error': 'Form not found'}), 404

    TEXT_SECTION_TYPES = {'text_questions', 'multiple_choice', 'yes_no'}
    text_q_map = {}
    for section in form.get('sections', []):
        s_type = section.get('type', '')
        if s_type in TEXT_SECTION_TYPES:
            for q in section.get('questions', []):
                text_q_map[q['id']] = q.get('text', q['id'])

    if not text_q_map:
        return jsonify({'questions': [], 'total_rows': 0,
                        'filtered_rows': 0, 'courses_available': []})

    try:
        dt_from = datetime.strptime(date_from_str, '%Y-%m-%d') if date_from_str else None
    except ValueError:
        dt_from = None
    try:
        dt_to = datetime.strptime(date_to_str, '%Y-%m-%d') if date_to_str else None
    except ValueError:
        dt_to = None

    rows = db.get_responses_for_analysis(form_id, form, dt_from,  dt_to, course_filter)

    text_agg = {}
    for row in rows:
        answers = row.get('answers', {})
        course_val = row.get('class_code', '')
        date_val = str(row.get('course_date', '') or '')
        for qid, qtext in text_q_map.items():
            val = str(answers.get(qid, '') or '').strip()
            if not val or val.lower() in ('none', '-', 'n/a', ''):
                continue
            if qid not in text_agg:
                text_agg[qid] = {'id': qid, 'text': qtext, 'responses': []}
            text_agg[qid]['responses'].append({'value': val, 'course': course_val, 'date': date_val})

    questions_out = list(text_agg.values())
    q_order = {q['id']: i for i, q in enumerate(
        q for sec in form.get('sections', []) for q in sec.get('questions', [])
    )}
    questions_out.sort(key=lambda x: q_order.get(x['id'], 9999))

    courses_available = db.get_distinct_courses_for_form(form_id, form)
    return jsonify({
        'questions': questions_out,
        'total_rows': len(rows),
        'filtered_rows': len(rows),
        'courses_available': courses_available
    })

def _parse_month_range(month_value):
    if not month_value:
        return None, None
    try:
        start = datetime.strptime(month_value, '%Y-%m')
    except ValueError:
        return None, None
    if start.month == 12:
        end = datetime(start.year + 1, 1, 1)
    else:
        end = datetime(start.year, start.month + 1, 1)
    return start, end

def _build_rating_question_map(form):
    """Build ordered map {column_id: question_text} for all rating-like sections."""
    rating_q_map = {}
    for section in form.get('sections', []):
        s_type = section.get('type', '')
        questions = section.get('questions', [])
        if s_type == 'instructor_rating':
            max_inst = section.get('maxInstructors', 3)
            for i in range(1, max_inst + 1):
                for q in questions:
                    col = f"B{i}_{q['id']}"
                    rating_q_map[col] = f"Instructor {i}: {q.get('text', q['id'])}"
        elif s_type == 'assessor_rating':
            max_assess = section.get('maxAssessors', 2)
            for i in range(1, max_assess + 1):
                for q in questions:
                    col = f"A{i}_{q['id']}"
                    rating_q_map[col] = f"Assessor {i}: {q.get('text', q['id'])}"
        elif s_type == 'rating':
            for q in questions:
                rating_q_map[q['id']] = q.get('text', q['id'])
    return rating_q_map

def _looks_like_uuid(value):
    s = str(value or '').strip()
    return bool(re.fullmatch(r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}', s))

def _resolve_analysis_class_code(row, course_id_to_class_code):
    class_code = str(row.get('class_code', '') or '').strip()
    raw_course_title = str(row.get('course_title', '') or '').strip()
    raw_course_id = str(row.get('course_id', '') or '').strip()

    if class_code and not _looks_like_uuid(class_code):
        return class_code

    mapped_code = str(course_id_to_class_code.get(raw_course_id, '') or '').strip()
    if mapped_code and not _looks_like_uuid(mapped_code):
        return mapped_code

    if raw_course_title and not _looks_like_uuid(raw_course_title):
        return raw_course_title

    return ''

@app.route('/api/analysis/dashboard/filters', methods=['GET'])
@api_login_required
def get_analysis_dashboard_filters():
    """Return dashboard filter options by form/month."""
    form_id = request.args.get('form_id', 'form1')
    month = request.args.get('month', '').strip()

    config = load_config()
    form = config['forms'].get(form_id)
    if not form:
        return jsonify({'error': 'Form not found'}), 404

    dt_from, dt_to = _parse_month_range(month)
    months_available = db.get_available_analysis_months(form_id, form)
    rows = db.get_responses_for_analysis(form_id, form, dt_from, dt_to, None)
    rating_q_map = _build_rating_question_map(form)

    course_ids = {r.get('course_id', '') for r in rows if r.get('course_id')}
    course_id_to_class_code = db.get_fbs_course_title_map(course_ids)

    normalized_codes = {
        str(_resolve_analysis_class_code(r, course_id_to_class_code) or '').strip().upper()
        for r in rows
    }
    normalized_codes.discard('')

    nav_name_map = db.get_nav_course_name_map(normalized_codes)

    course_titles = set()
    question_counts = defaultdict(int)
    for row in rows:
        class_code = str(_resolve_analysis_class_code(row, course_id_to_class_code) or '').strip()
        class_code_key = class_code.upper()
        course_title = (nav_name_map.get(class_code_key) or class_code or '').strip()
        if course_title:
            course_titles.add(course_title)

        answers = row.get('answers', {})
        for qid in rating_q_map.keys():
            val = answers.get(qid)
            try:
                rating = int(float(str(val))) if val is not None else None
            except (ValueError, TypeError):
                rating = None
            if rating is not None and 1 <= rating <= 5:
                question_counts[qid] += 1

    questions_available = [
        {
            'id': qid,
            'text': qtext,
            'count': question_counts.get(qid, 0)
        }
        for qid, qtext in rating_q_map.items()
        if question_counts.get(qid, 0) > 0
    ]

    return jsonify({
        'months_available': months_available,
        'course_titles_available': sorted(course_titles),
        'questions_available': questions_available
    })


@app.route('/api/analysis/dashboard', methods=['GET'])
@api_login_required
def get_analysis_dashboard():
    """Return pie/bar-ready rating analysis for the dashboard UI."""
    form_id = request.args.get('form_id', 'form1')
    month = request.args.get('month', '').strip()
    selected_questions = [q.strip() for q in request.args.getlist('question') if q.strip()]
    selected_course_titles = [c.strip() for c in request.args.getlist('course_title') if c.strip()]
    sort_mode = request.args.get('sort', 'priority').strip().lower()

    config = load_config()
    form = config['forms'].get(form_id)
    if not form:
        return jsonify({'error': 'Form not found'}), 404

    dt_from, dt_to = _parse_month_range(month)
    months_available = db.get_available_analysis_months(form_id, form)
    rows = db.get_responses_for_analysis(form_id, form, dt_from, dt_to, None)
    rating_q_map = _build_rating_question_map(form)

    course_ids = {r.get('course_id', '') for r in rows if r.get('course_id')}
    course_id_to_class_code = db.get_fbs_course_title_map(course_ids)

    normalized_codes = {
        str(_resolve_analysis_class_code(r, course_id_to_class_code) or '').strip().upper()
        for r in rows
    }
    normalized_codes.discard('')

    nav_name_map = db.get_nav_course_name_map(normalized_codes)

    course_titles_available = set()
    prepared_rows = []
    for row in rows:
        class_code = str(_resolve_analysis_class_code(row, course_id_to_class_code) or '').strip()
        class_code_key = class_code.upper()
        resolved_title = (nav_name_map.get(class_code_key) or class_code or 'Unknown Course').strip()
        if resolved_title:
            course_titles_available.add(resolved_title)
        prepared_rows.append({
            'course_title_resolved': resolved_title,
            'answers': row.get('answers', {})
        })

    if selected_course_titles:
        selected_set = set(selected_course_titles)
        prepared_rows = [r for r in prepared_rows if r.get('course_title_resolved') in selected_set]

    question_ids = selected_questions if selected_questions else list(rating_q_map.keys())

    rating_labels = {
        1: 'Not at all',
        2: 'To a Small Extent',
        3: 'To Some Extent',
        4: 'To a Large Extent',
        5: 'To a Very Large Extent'
    }
    rating_colors = {
        1: '#dc3545',
        2: '#fd7e14',
        3: '#ffc107',
        4: '#4f8ad9',
        5: '#28a745'
    }

    overall_dist = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    per_question = {}
    for qid in question_ids:
        qtext = rating_q_map.get(qid)
        if qtext:
            per_question[qid] = {
                'id': qid,
                'text': qtext,
                'count': 0,
                'total': 0,
                'avg': 0,
                'good_pct': 0,
                'dist': {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
            }

    for row in prepared_rows:
        answers = row.get('answers', {})
        for qid in question_ids:
            if qid not in per_question:
                continue
            val = answers.get(qid)
            try:
                rating = int(float(str(val))) if val is not None else None
            except (ValueError, TypeError):
                rating = None
            if rating is None or rating < 1 or rating > 5:
                continue
            per_question[qid]['count'] += 1
            per_question[qid]['total'] += rating
            per_question[qid]['dist'][rating] += 1
            overall_dist[rating] += 1

    questions_available = []
    for qid, qtext in rating_q_map.items():
        count = per_question[qid]['count'] if qid in per_question else 0
        if qid not in per_question:
            for row in prepared_rows:
                val = row.get('answers', {}).get(qid)
                try:
                    rating = int(float(str(val))) if val is not None else None
                except (ValueError, TypeError):
                    rating = None
                if rating is not None and 1 <= rating <= 5:
                    count += 1
        if count > 0:
            questions_available.append({'id': qid, 'text': qtext, 'count': count})

    for q in per_question.values():
        if q['count'] > 0:
            q['avg'] = round(q['total'] / q['count'], 2)
            q['good_pct'] = round(((q['dist'][3] + q['dist'][4] + q['dist'][5]) / q['count']) * 100, 2)

    ranked_questions = [q for q in per_question.values() if q['count'] > 0]

    if sort_mode == 'avg_asc':
        ranked_questions.sort(key=lambda x: (x['avg'], -x['count'], x['text']))
    elif sort_mode == 'avg_desc':
        ranked_questions.sort(key=lambda x: (-x['avg'], -x['count'], x['text']))
    elif sort_mode == 'count_desc':
        ranked_questions.sort(key=lambda x: (-x['count'], x['avg'], x['text']))
    else:
        ranked_questions.sort(key=lambda x: (x['avg'], -x['count'], x['text']))

    total_ratings = sum(overall_dist.values())
    good_count = overall_dist[3] + overall_dist[4] + overall_dist[5]
    bad_count = overall_dist[1] + overall_dist[2]
    good_pct = round((good_count / total_ratings) * 100, 2) if total_ratings else 0

    active_ratings = [r for r in [1, 2, 3, 4, 5] if any(q['dist'][r] > 0 for q in ranked_questions)]
    bar_datasets = [
        {
            'label': rating_labels[r],
            'rating_value': r,
            'backgroundColor': rating_colors[r],
            'data': [q['dist'][r] for q in ranked_questions]
        }
        for r in active_ratings
    ]

    top_issues = [
        {
            'id': q['id'],
            'text': q['text'],
            'avg': q['avg'],
            'count': q['count'],
            'good_pct': q['good_pct']
        }
        for q in ranked_questions[:5]
    ]
    strongest_questions = [
        {
            'id': q['id'],
            'text': q['text'],
            'avg': q['avg'],
            'count': q['count'],
            'good_pct': q['good_pct']
        }
        for q in sorted(ranked_questions, key=lambda x: (-x['avg'], -x['count'], x['text']))[:5]
    ]

    return jsonify({
        'filters': {
            'months_available': months_available,
            'course_titles_available': sorted(course_titles_available),
            'questions_available': questions_available
        },
        'summary': {
            'selected_month': month,
            'response_count': len(prepared_rows),
            'total_ratings': total_ratings,
            'rating_3_or_above_pct': good_pct,
            'question_count': len(ranked_questions),
            'sort_mode': sort_mode
        },
        'pie': {
            'labels': ['Good Satisfaction (>=3)', 'Bad Satisfaction (<3)'],
            'values': [good_count, bad_count],
            'colors': ['#16a34a', '#ef4444']
        },
        'bar': {
            'question_labels': [q['text'] for q in ranked_questions],
            'datasets': bar_datasets,
            'questions': ranked_questions
        },
        'insights': {
            'top_issues': top_issues,
            'strongest_questions': strongest_questions
        },
        'rating_legend': rating_labels
    })


@app.route('/api/forms/<form_id>/structure', methods=['GET'])
@api_login_required
def get_form_structure(form_id):
    """Get the structure (sections and questions) of an existing form for copying/editing."""
    config = load_config()
    if form_id not in config['forms']:
        return jsonify({'error': 'Form not found'}), 404
    
    form = config['forms'][form_id]
    return jsonify({
        'success': True,
        'form_id': form_id,
        'title': form.get('title', ''),
        'language': form.get('language', 'English'),
        'sections': form.get('sections', []),
        'headerFields': form.get('headerFields', []),
        'ratingOptions': form.get('ratingOptions', [])
    })


@app.route('/api/forms', methods=['POST'])
@api_login_required
def create_form():
    """Create a new custom form. Form ID is auto-generated from title + language."""
    config = load_config()
    data = request.json
    title = (data.get('title') or 'New Form').strip()
    language = (data.get('language') or 'English').strip()
    
    base_form_id = _slugify_form_id(title)
    lang_code = _language_to_code(language)
    form_id = f"{base_form_id}_{lang_code}"
    
    if form_id in config['forms']:
        return jsonify({'error': f'Form "{title}" in language "{language}" already exists.'}), 400

    template_id = data.get('copy_from', '')
    qr_fields = data.get('qr_fields')
    auto_copy_from_id = None
    if not template_id:
        for fid, f in config['forms'].items():
            if f.get('title') == title and f.get('base_form_id') == base_form_id:
                auto_copy_from_id = fid
                break
    
    if template_id or auto_copy_from_id:
        source_form_id = template_id or auto_copy_from_id
        if source_form_id in config['forms']:
            new_form = copy.deepcopy(config['forms'][source_form_id])
            new_form['id'] = form_id
            new_form['title'] = title
            new_form['language'] = language
            new_form['base_form_id'] = base_form_id
            new_form['formNumber'] = data.get('formNumber', new_form.get('formNumber', ''))
            new_form['description'] = data.get('description', new_form.get('description', ''))
    
            if data.get('sections'):
                new_form['sections'] = data.get('sections', [])
        
        if qr_fields:
            new_form['qr_fields'] = qr_fields
            sections = new_form.get('sections', [])
            
            if qr_fields.get('instructors', {}).get('show'):
                has_instructor = any(s.get('type') == 'instructor_rating' for s in sections)
                if not has_instructor:
                    max_inst = qr_fields.get('instructors', {}).get('max', 3)
                    inst_section = _get_default_instructor_section(max_inst)
                    inst_section['id'] = _get_unique_section_id(sections, 'B')
                    sections.insert(0, inst_section)
            else:
                sections = [s for s in sections if s.get('type') != 'instructor_rating']
            
            if qr_fields.get('assessors', {}).get('show'):
                has_assessor = any(s.get('type') == 'assessor_rating' for s in sections)
                if not has_assessor:
                    max_assess = qr_fields.get('assessors', {}).get('max', 2)
                    assess_section = _get_default_assessor_section(max_assess)
                    assess_section['id'] = _get_unique_section_id(sections, 'A')
                    sections.insert(0, assess_section)
            else:
                sections = [s for s in sections if s.get('type') != 'assessor_rating']
            
            new_form['sections'] = sections
    else:
        header_fields = [
            {'id': 'course_title', 'label': 'Course Title', 'type': 'text', 'required': True, 'prefilled': True},
            {'id': 'course_date', 'label': 'Course Date', 'type': 'date', 'required': True, 'prefilled': True},
        ]
        if qr_fields:
            if qr_fields.get('classroom', {}).get('show'):
                lbl = qr_fields['classroom'].get('label', 'Classroom')
                header_fields.append({'id': 'classroom', 'label': lbl, 'type': 'text', 'required': True, 'prefilled': True})
            if qr_fields.get('assessment_location', {}).get('show'):
                lbl = qr_fields['assessment_location'].get('label', 'Assessment Location')
                header_fields.append({'id': 'assessment_location', 'label': lbl, 'type': 'text', 'required': True, 'prefilled': True})
            for cf in qr_fields.get('custom_fields', []):
                header_fields.append({'id': cf['id'], 'label': cf['label'], 'type': 'text', 'required': cf.get('required', False), 'prefilled': True})
        else:
            header_fields.append({'id': 'classroom', 'label': 'Classroom', 'type': 'text', 'required': True, 'prefilled': True})
        header_fields += [
            {'id': 'name', 'label': 'Name', 'type': 'text', 'required': False, 'prefilled': False},
            {'id': 'position', 'label': 'Position', 'type': 'text', 'required': True, 'prefilled': False},
        ]
        new_form = {
            'id': form_id,
            'title': title,
            'language': language,
            'base_form_id': base_form_id,
            'formNumber': data.get('formNumber', ''),
            'description': data.get('description', ''),
            'headerFields': header_fields,
            'ratingOptions': [
                {'value': 1, 'label': 'Poor'},
                {'value': 2, 'label': 'Unsatisfactory'},
                {'value': 3, 'label': 'Satisfactory'},
                {'value': 4, 'label': 'Very Good'},
                {'value': 5, 'label': 'Excellent'},
            ],
            'sections': data.get('sections', [])
        }
        if qr_fields:
            sections = new_form.get('sections', [])
            
            if qr_fields.get('instructors', {}).get('show'):
                has_instructor = any(s.get('type') == 'instructor_rating' for s in sections)
                if not has_instructor:
                    max_inst = qr_fields.get('instructors', {}).get('max', 3)
                    inst_section = _get_default_instructor_section(max_inst)
                    inst_section['id'] = _get_unique_section_id(sections, 'B')
                    sections.insert(0, inst_section)
            
            if qr_fields.get('assessors', {}).get('show'):
                has_assessor = any(s.get('type') == 'assessor_rating' for s in sections)
                if not has_assessor:
                    max_assess = qr_fields.get('assessors', {}).get('max', 2)
                    assess_section = _get_default_assessor_section(max_assess)
                    assess_section['id'] = _get_unique_section_id(sections, 'A')
                    sections.insert(0, assess_section)
            
            new_form['sections'] = sections
            new_form['qr_fields'] = qr_fields

    form_ok = db.register_form(
        form_id,
        title,
        new_form.get('formNumber', ''),
        new_form.get('description', ''),
        new_form,
    )
    if not form_ok:
        return jsonify({'error': 'Could not save new form to FBS_Forms'}), 500
    return jsonify({'success': True, 'form': new_form})

@app.route('/api/forms/<form_id>', methods=['DELETE'])
@api_login_required
def delete_form(form_id):
    """Delete or archive a form. Forms with response data are archived so that 
    historical responses are never lost. Forms without data are hard-deleted
    and their empty response tables are removed."""
    if form_id in ('form1', 'form2'):
        return jsonify({'error': 'Cannot delete built-in forms'}), 400
    config = load_config()
    if form_id not in config['forms']:
        return jsonify({'error': 'Form not found'}), 404

    form_title = config['forms'].get(form_id, {}).get('title', form_id)
    if db.form_has_responses(form_id, form_title):
        if not db.soft_delete_form(form_id):
            return jsonify({'error': 'Could not archive form in FBS_Forms'}), 500
        return jsonify({
            'success': True,
            'archived': True,
            'message': 'Form archived — all responses are preserved in the database.'
        })
    else:
        drop_ok, dropped, drop_msg = db.drop_form_response_table_if_empty(form_title)

        if not drop_ok:
            return jsonify({'error': f'Could not remove response table: {drop_msg}'}), 500

        if not dropped and db.form_has_responses(form_id, form_title):
            if not db.soft_delete_form(form_id):
                return jsonify({'error': 'Could not archive form in FBS_Forms'}), 500
            return jsonify({
                'success': True,
                'archived': True,
                'message': 'Form archived — responses were detected, table/data preserved.'
            })

        if not db.soft_delete_form(form_id):
            return jsonify({'error': 'Could not mark form deleted in FBS_Forms'}), 500
        return jsonify({'success': True, 'archived': False, 'message': drop_msg})

@app.route('/low-ratings')
@api_login_required
def low_ratings_page():
    """Render the low ratings feedback page."""
    config = load_config()
    forms_dict = db.get_active_forms_map()
    
    return render_template('low_ratings.html', forms=forms_dict)


@app.route('/api/low-ratings-data')
@api_login_required
def get_low_ratings_data():
    forms_str = request.args.get('forms', '').strip()
    rating_threshold = int(request.args.get('rating_threshold', 2))
    question_filter = request.args.get('question_id', '').strip()
    include_alerts = request.args.get('include_alerts', 'true').lower() == 'true'
    
    if not forms_str:
        return jsonify({'error': 'No forms specified'}), 400
    
    form_ids = [f.strip() for f in forms_str.split(',') if f.strip()]
    forms_dict = db.get_active_forms_map()
    
    alerts_by_qid = {}
    if include_alerts:
        alerts = load_alerts()
        for alert in alerts:
            qid = alert.get('question_id', '')
            if qid not in alerts_by_qid:
                alerts_by_qid[qid] = []
            alerts_by_qid[qid].append(alert)
    
    # Get low ratings
    all_low_ratings = []
    for form_id in form_ids:
        if form_id in forms_dict:
            form_config = forms_dict[form_id]
            responses = db.get_low_rating_responses(form_id, form_config, rating_threshold)
            
            for r in responses:
                for rating in r.get('ratings', []):
                    q_id = rating['question_id']
                    related_alerts = alerts_by_qid.get(q_id, [])
                    rating['alert_status'] = related_alerts[0].get('status', 'new') if related_alerts else 'new'
                    rating['alert_notes'] = related_alerts[0].get('action_notes', '') if related_alerts else ''
            
            if question_filter:
                responses = [
                    {**r, 'ratings': [rat for rat in r['ratings'] if rat['question_id'] == question_filter]}
                    for r in responses
                ]
                responses = [r for r in responses if r['ratings']]
            
            all_low_ratings.extend(responses)
    
    all_text_responses = []
    for form_id in form_ids:
        if form_id in forms_dict:
            form_config = forms_dict[form_id]
            responses = db.get_text_question_responses(form_id, form_config)
            
            # Add alert status to each text response
            for r in responses:
                filtered_text_responses = []
                for text_resp in r.get('text_responses', []):
                    q_id = text_resp['question_id']
                    related_alerts = alerts_by_qid.get(q_id, [])
                    active_alerts = [a for a in related_alerts if a.get('status') != 'resolved']

                    if active_alerts:
                        text_resp['alert_status'] = active_alerts[0].get('status', 'new')
                        text_resp['alert_notes'] = active_alerts[0].get('action_notes', '')
                        filtered_text_responses.append(text_resp)
                
                if filtered_text_responses:
                    r['text_responses'] = filtered_text_responses
                    all_text_responses.append(r)
    
    all_feedback = all_low_ratings + all_text_responses
    for item in all_feedback:
        if isinstance(item.get('submission_time'), datetime):
            item['submission_time'] = item['submission_time'].strftime('%Y-%m-%d %H:%M:%S')
    
    return jsonify(all_feedback)


@app.route('/api/rating-questions')
@api_login_required
def get_rating_questions():
    """Get all rating questions for filtering."""
    forms_str = request.args.get('forms', '').strip()
    
    if not forms_str:
        return jsonify({}), 400
    
    form_ids = [f.strip() for f in forms_str.split(',') if f.strip()]
    forms_dict = db.get_active_forms_map()
    filtered_forms = {fid: forms_dict[fid] for fid in form_ids if fid in forms_dict}
    questions_by_form = db.get_all_rating_questions_by_form(filtered_forms)
    return jsonify(questions_by_form)


@app.route('/api/hotspot-analysis')
@api_login_required
def get_hotspot_analysis():
    """Get hotspot analysis - questions ranked by priority. Includes BOTH rating questions and text questions."""
    forms_str = request.args.get('forms', '').strip()
    
    alerts = load_alerts()
    active = [a for a in alerts if a.get('status') != 'resolved']
    
    if forms_str:
        form_ids = {f.strip() for f in forms_str.split(',') if f.strip()}
        active = [a for a in active if a.get('form_id') in form_ids]
    
    groups = defaultdict(lambda: {
        'question_id': '', 'question_text': '', 'question_type': '', 'forms': set(),
        'alert_count': 0, 'avg_rating': 0, 'alert_ids': [], 'comments': []
    })
    
    for a in active:
        qid = a.get('question_id', '')
        alert_type = a.get('alert_type', 'rating')
        g = groups[qid]
        g['question_id'] = qid
        g['question_text'] = g['question_text'] or a.get('question_text', qid)
        g['question_type'] = g['question_type'] or alert_type
        g['forms'].add(a.get('form_id', ''))
        g['alert_ids'].append(a.get('id'))
        g['alert_count'] += 1
        
        if alert_type == 'rating' and 'rating' in a:
            if not hasattr(g, '_ratings'):
                g['_ratings'] = []
            g['_ratings'].append(a.get('rating', 0))
        
        if a.get('comment'):
            g['comments'].append(a['comment'])
    
    result = []
    for qid, g in groups.items():
        ratings = g.get('_ratings', [3])
        avg_rating = sum(ratings) / len(ratings) if ratings else 3
        urgency = max(0, 3 - avg_rating) + 1 
        priority = round(g['alert_count'] * urgency, 2)
        
        result.append({
            'question_id': g['question_id'],
            'question_text': g['question_text'],
            'question_type': g['question_type'],
            'forms': list(g['forms']),
            'alert_count': g['alert_count'],
            'avg_rating': round(avg_rating, 2),
            'priority_score': priority,
            'alert_ids': g['alert_ids'],
            'comments': g['comments'][:5]
        })
    
    result.sort(key=lambda x: x['priority_score'], reverse=True)
    return jsonify(result)


@app.route('/api/update-alert-status', methods=['PUT'])
@api_login_required
def update_alert_status():
    """Update alert status and notes."""
    data = request.get_json()
    alert_ids = data.get('alert_ids', [])
    status = data.get('status', '').strip()
    notes = data.get('notes', '').strip()
    
    if not alert_ids or not status:
        return jsonify({'error': 'alert_ids and status required'}), 400
    
    alerts = load_alerts()
    updated_count = 0
    
    for alert in alerts:
        if alert.get('id') in alert_ids:
            alert['status'] = status
            if notes:
                alert['action_notes'] = notes
            alert['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            updated_count += 1
    
    save_alerts_data(alerts)
    return jsonify({'success': True, 'updated': updated_count})


@app.route('/api/send-rectification', methods=['POST'])
@api_login_required
def send_rectification():
    try:
        data = request.get_json()
        
        form_id = data.get('form_id', '').strip()
        response_id = data.get('response_id', '').strip()
        question_id = data.get('question_id', '').strip()
        participant_email = data.get('participant_email', '').strip()
        rectification_text = data.get('rectification_text', '').strip()
        implementation_date = data.get('implementation_date', '').strip()
        status = data.get('status', 'Pending').strip()
        
        if not all([form_id, response_id, question_id, participant_email]):
            return jsonify({'error': 'Missing required fields'}), 400
        
        if db.check_rectification_already_sent(form_id, response_id, question_id):
            return jsonify({'error': 'Rectification already sent for this low rating'}), 409
        
        question_text = data.get('question_text', '')
        rating_value = data.get('rating_value', 0)
        participant_name = data.get('participant_name', '')
        email_subject = f"Response to Your Feedback"
        email_body = f"""Dear {participant_name},

Thank you for completing the feedback form. We appreciate your valuable input.

Question: {question_text}

Rectification / Response:
{rectification_text}

Implementation Date: {implementation_date}
Status: {status}

We take your feedback seriously and will work to improve our services.

Best regards,
AKC Training Team
postcourse.enquiries@SG-AKC.com"""
        
        from urllib.parse import quote
        subject_encoded = quote(email_subject)
        body_encoded = quote(email_body)
        mailto_link = f"mailto:{participant_email}?subject={subject_encoded}&body={body_encoded}"
        
        import uuid
        try:
            response_uuid = uuid.UUID(response_id)
        except ValueError:
            return jsonify({'error': 'Invalid response_id format'}), 400
        
        log_ok = db.log_rectification_sent(
            form_id=form_id,
            response_id=response_uuid,
            participant_name=participant_name,
            participant_email=participant_email,
            question_id=question_id,
            question_text=question_text,
            rating_value=rating_value,
            rectification_text=rectification_text,
            implementation_date=implementation_date if implementation_date else None,
            status=status
        )
        
        if not log_ok:
            return jsonify({'error': 'Could not log rectification'}), 500
        
        return jsonify({
            'success': True,
            'message': 'Rectification logged successfully',
            'mailto_link': mailto_link,
            'email_body': email_body
        })
    
    except Exception as e:
        print(f"Error sending rectification: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    db.init_fbs_tables()
    db.init_rectification_log_table()
    _cfg = load_config()
    _forms_ok, _forms_sync = sync_forms_registry_with_config(_cfg)
    print(f"[FBS_Forms sync] {sum(1 for ok in _forms_sync.values() if ok)}/{len(_forms_sync)} forms synced")
    for _fid, _form in _cfg['forms'].items():
        if not _form.get('is_archived'):
            _ok, _msg = db.create_form_response_table(_form.get('title', _fid), _form)
            print(f"  [{_form.get('title', _fid)}]: {_msg}")
    app.run(debug=True, host='0.0.0.0', port=5000)
