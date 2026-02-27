from flask import Flask, render_template, request, jsonify, redirect, url_for
import json
import os
from datetime import datetime, timedelta
from openpyxl import Workbook, load_workbook
import uuid
from dotenv import load_dotenv
load_dotenv()
import db

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config.json')
DATA_DIR = os.path.join(BASE_DIR, 'data')

# Course link expiration time
COURSE_EXPIRY_HOURS = 1

os.makedirs(DATA_DIR, exist_ok=True)


def load_config():
    """Load configuration from JSON file"""
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_config(config):
    """Save configuration to JSON file"""
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def clean_expired_courses():
    """Remove courses that are older than COURSE_EXPIRY_HOURS (default 24 hours)"""
    config = load_config()
    now = datetime.now()
    
    original_count = len(config['courses'])
    valid_courses = []
    
    for course in config['courses']:
        created_at = course.get('created_at', '')
        if created_at:
            try:
                created_time = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
                age_hours = (now - created_time).total_seconds() / 3600
                
                if age_hours < COURSE_EXPIRY_HOURS:
                    valid_courses.append(course)
                else:
                    print(f"Removing expired course: {course.get('course_title', 'Unknown')} (created {age_hours:.1f} hours ago)")
            except ValueError:
                valid_courses.append(course)
        else:
            valid_courses.append(course)
    
    removed_count = original_count - len(valid_courses)
    
    if removed_count > 0:
        config['courses'] = valid_courses
        save_config(config)
        print(f"Cleaned up {removed_count} expired course(s)")
    
    return removed_count

FORM_FILE_NAMES = {
    'form1': 'Trainer_Evaluation',
    'form2': 'Assessor_Evaluation'
}


def get_excel_path(form_id):
    """Get the Excel file path for a form"""
    file_name = FORM_FILE_NAMES.get(form_id, form_id)
    return os.path.join(DATA_DIR, f'{file_name}_responses.xlsx')


def init_excel(form_id):
    """Initialize Excel file with headers if it doesn't exist"""
    excel_path = get_excel_path(form_id)
    
    if os.path.exists(excel_path):
        return
    
    config = load_config()
    form = config['forms'].get(form_id)
    if not form:
        return
    
    wb = Workbook()
    ws = wb.active
    ws.title = "Responses"
    
    if form_id == 'form1':
        # Trainer Evaluation form 
        headers = [
            'COURSE NAME',
            'DATE',
            'CLASSROOM',
            'INSTRUCTOR 1',
            'INSTRUCTOR 2',
            'INSTRUCTOR 3',
            'LANGUAGE'
        ]
        
        for section in form['sections']:
            if section['type'] == 'rating':
                for q in section['questions']:
                    headers.append(f"{q['id']} - {q['text']}")
            elif section['type'] == 'instructor_rating':
                for q in section['questions']:
                    headers.append(f"B1{q['id']} - {q['text']}")
                for inst_num in range(2, 4):
                    for q in section['questions']:
                        headers.append(f"B{inst_num}{q['id']}")
            elif section['type'] == 'text_questions':
                for q in section['questions']:
                    headers.append(f"{q['id']} - {q['text']}")
    elif form_id == 'form2':
        # Assessor Evaluation form
        headers = [
            'COURSE NAME',
            'DATE',
            'CLASSROOM',
            'ASSESSOR 1',
            'ASSESSOR 2',
            'LANGUAGE'
        ]
        
        for section in form['sections']:
            if section['type'] == 'assessor_rating':
                for q in section['questions']:
                    headers.append(f"A1.{q['id']} - {q['text']}")
                for q in section['questions']:
                    headers.append(f"A2.{q['id']}")
            elif section['type'] == 'text_questions':
                for q in section['questions']:
                    headers.append(f"{q['id']} - {q['text']}")
    else:
        headers = ['Submission Time']
        
        for field in form['headerFields']:
            headers.append(field['label'].replace(' (Optional)', ''))
        
        for section in form['sections']:
            if section['type'] == 'rating':
                for q in section['questions']:
                    headers.append(q['id'])
            elif section['type'] == 'instructor_rating':
                # Add columns for up to 3 instructors
                for i in range(1, 4):
                    headers.append(f'Instructor {i} Name')
                    for q in section['questions']:
                        headers.append(f'B{i}-{q["id"]}')
            elif section['type'] == 'assessor_rating':
                # Add columns for up to 2 assessors
                for i in range(1, 3):
                    headers.append(f'Assessor {i} Name')
                    for q in section['questions']:
                        headers.append(f'A{i}-{q["id"]}')
            elif section['type'] == 'text_questions':
                for q in section['questions']:
                    headers.append(q['id'])
    
    ws.append(headers)
    wb.save(excel_path)

def save_response(form_id, course_id, data):
    """Save form response to Excel and database"""
    excel_path = get_excel_path(form_id)
    init_excel(form_id)
    
    config = load_config()
    form = config['forms'].get(form_id)
    
    course = None
    for c in config['courses']:
        if c['id'] == course_id:
            course = c
            break
    
    # Save to Excel
    wb = load_workbook(excel_path)
    ws = wb.active
    existing_headers = [cell.value for cell in ws[1]]
    data_map = {}
    
    if form_id == 'form1':
        instructors = course.get('instructors', []) if course else []
        data_map['COURSE NAME'] = course['course_title'] if course else ''
        data_map['DATE'] = course['course_date'] if course else ''
        data_map['CLASSROOM'] = course.get('classroom', '') if course else ''
        data_map['INSTRUCTOR 1'] = instructors[0] if len(instructors) > 0 else ''
        data_map['INSTRUCTOR 2'] = instructors[1] if len(instructors) > 1 else ''
        data_map['INSTRUCTOR 3'] = instructors[2] if len(instructors) > 2 else ''
        data_map['LANGUAGE'] = ''
        
        for section in form['sections']:
            if section['type'] == 'rating':
                for q in section['questions']:
                    data_map[q['id']] = data.get(q['id'], '')
            elif section['type'] == 'instructor_rating':
                for inst_num in range(1, 4):
                    for q in section['questions']:
                        key = f"B{inst_num}{q['id']}"
                        data_map[key] = data.get(f'B{inst_num}_{q["id"]}', '')
            elif section['type'] == 'text_questions':
                for q in section['questions']:
                    data_map[q['id']] = data.get(q['id'], '')
                    
    elif form_id == 'form2':
        assessors = course.get('assessors', []) if course else []
        data_map['COURSE NAME'] = course['course_title'] if course else ''
        data_map['DATE'] = course['course_date'] if course else ''
        data_map['CLASSROOM'] = course.get('classroom', '') if course else ''
        data_map['ASSESSOR 1'] = assessors[0] if len(assessors) > 0 else ''
        data_map['ASSESSOR 2'] = assessors[1] if len(assessors) > 1 else ''
        data_map['LANGUAGE'] = ''
        
        for section in form['sections']:
            if section['type'] == 'assessor_rating':
                for i in range(1, 3):
                    for q in section['questions']:
                        key = f"A{i}.{q['id']}"
                        data_map[key] = data.get(f'A{i}_{q["id"]}', '')
            elif section['type'] == 'text_questions':
                for q in section['questions']:
                    data_map[q['id']] = data.get(q['id'], '')
    
    row = []
    for header in existing_headers:
        if header is None or header.startswith('[REMOVED]'):
            row.append('')
        else:
            if header in data_map:
                row.append(data_map[header])
            else:
                header_id = header.split(' - ')[0].strip() if ' - ' in header else header
                if header_id in data_map:
                    row.append(data_map[header_id])
                else:
                    row.append('') 
    
    ws.append(row)
    wb.save(excel_path)

    try:
        db.save_to_database(form_id, course_id, course, data)
    except Exception as e:
        print(f"Warning: Could not save to database: {e}")

@app.route('/')
def index():
    """Redirect to admin page"""
    return redirect(url_for('admin'))


@app.route('/admin')
def admin():
    """Admin dashboard"""
    clean_expired_courses()
    config = load_config()
    return render_template('admin.html', config=config)


@app.route('/admin/form/<form_id>')
def admin_form(form_id):
    """Admin page for editing a specific form"""
    config = load_config()
    form = config['forms'].get(form_id)
    if not form:
        return "Form not found", 404
    return render_template('admin_form.html', form=form, config=config)


@app.route('/api/forms/<form_id>', methods=['GET'])
def get_form(form_id):
    """Get form configuration"""
    config = load_config()
    form = config['forms'].get(form_id)
    if not form:
        return jsonify({'error': 'Form not found'}), 404
    return jsonify(form)


@app.route('/api/forms/<form_id>', methods=['PUT'])
def update_form(form_id):
    """Update form configuration"""
    config = load_config()
    if form_id not in config['forms']:
        return jsonify({'error': 'Form not found'}), 404
    
    data = request.json
    config['forms'][form_id] = data
    save_config(config)
    return jsonify({'success': True})


@app.route('/api/forms/<form_id>/sections', methods=['POST'])
def add_section(form_id):
    """Add a new section to form"""
    config = load_config()
    if form_id not in config['forms']:
        return jsonify({'error': 'Form not found'}), 404
    
    data = request.json
    config['forms'][form_id]['sections'].append(data)
    save_config(config)
    return jsonify({'success': True})


@app.route('/api/forms/<form_id>/sections/<section_id>/questions', methods=['POST'])
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
    
    save_config(config)
    return jsonify({'success': True})


@app.route('/api/forms/<form_id>/sections/<section_id>/questions/<question_id>', methods=['DELETE'])
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
    
    save_config(config)
    return jsonify({'success': True})


@app.route('/api/forms/<form_id>/update-excel', methods=['POST'])
def update_excel_columns(form_id):
    """Update Excel columns based on form changes"""
    try:
        excel_path = get_excel_path(form_id)
        
        if not os.path.exists(excel_path):
            return jsonify({'success': True, 'message': 'Excel file does not exist yet, will be created on first response'})
        
        changes = request.json
        added_questions = changes.get('addedQuestions', [])
        deleted_questions = changes.get('deletedQuestions', [])
        modified_questions = changes.get('modifiedQuestions', [])
        
        wb = load_workbook(excel_path)
        ws = wb.active
        headers = [cell.value for cell in ws[1]]
        
        for mod in modified_questions:
            q_id = mod['questionId']
            new_text = mod['newText']
            section_id = mod['sectionId']
            for col_idx, header in enumerate(headers):
                if header and header.startswith(f"{q_id} -"):
                    ws.cell(row=1, column=col_idx + 1).value = f"{q_id} - {new_text}"
                    headers[col_idx] = f"{q_id} - {new_text}"
                    break
    
        for added in added_questions:
            q_id = added['questionId']
            q_text = added['questionText']
            section_id = added['sectionId']
            new_header = f"{q_id} - {q_text}"
            new_col = len(headers) + 1
            ws.cell(row=1, column=new_col).value = new_header
            headers.append(new_header)
        
        columns_to_delete = []
        for deleted in deleted_questions:
            q_id = deleted['questionId']
            delete_data = deleted.get('deleteData', False)
            for col_idx, header in enumerate(headers):
                if header and (header.startswith(f"{q_id} -") or header == q_id):
                    if delete_data:
                        columns_to_delete.append(col_idx + 1)  
                    else:
                        ws.cell(row=1, column=col_idx + 1).value = f"[REMOVED] {header}"
                    break
        
        for col_idx in sorted(columns_to_delete, reverse=True):
            ws.delete_cols(col_idx)
        
        wb.save(excel_path)
        
        return jsonify({
            'success': True,
            'message': f'Excel updated: {len(added_questions)} added, {len(modified_questions)} modified, {len(deleted_questions)} deleted'
        })
        
    except Exception as e:
        print(f"Error updating Excel: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/db/test', methods=['GET'])
def test_db_connection():
    """Test database connection"""
    success, message = db.test_connection()
    return jsonify({'success': success, 'message': message})


@app.route('/api/db/courses', methods=['GET'])
def search_db_courses():
    """Search courses from database"""
    search_term = request.args.get('search', '')
    limit = request.args.get('limit', 50, type=int)
    courses = db.get_courses_from_db(search_term, limit)
    return jsonify(courses)


@app.route('/api/db/participants', methods=['GET'])
def get_participants():
    """Get participants for a class with pagination"""
    class_code = request.args.get('class_code', '')
    offset = request.args.get('offset', 0, type=int)
    limit = request.args.get('limit', 20, type=int)
    
    if not class_code:
        return jsonify({'error': 'Class code required'}), 400
    
    result = db.get_participants_by_class(class_code, offset, limit)
    return jsonify(result)


@app.route('/api/db/update-survey-sent', methods=['POST'])
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
def create_tables():
    """Create feedback tables in database"""
    success, message = db.create_feedback_tables()
    return jsonify({'success': success, 'message': message})


@app.route('/api/courses', methods=['GET'])
def get_courses():
    """Get all courses"""
    config = load_config()
    return jsonify(config['courses'])


@app.route('/api/courses', methods=['POST'])
def create_course():
    """Create a new course instance"""
    config = load_config()
    data = request.json
    
    course = {
        'id': str(uuid.uuid4())[:8],
        'form_id': data['form_id'],
        'course_title': data['course_title'],
        'course_date': data['course_date'],
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # Form-specific fields
    if data['form_id'] == 'form2':
        course['assessment_location'] = data.get('assessment_location', '')
        course['num_assessors'] = data.get('num_assessors', 1)
        course['assessors'] = data.get('assessors', [])
    else:
        course['classroom'] = data.get('classroom', '')
        course['num_instructors'] = data.get('num_instructors', 1)
        course['instructors'] = data.get('instructors', [])
    
    config['courses'].append(course)
    save_config(config)
    
    return jsonify({'success': True, 'course': course})


@app.route('/api/courses/<course_id>', methods=['DELETE'])
def delete_course(course_id):
    """Delete a course"""
    config = load_config()
    config['courses'] = [c for c in config['courses'] if c['id'] != course_id]
    save_config(config)
    return jsonify({'success': True})


@app.route('/form/<course_id>')
def form_page(course_id):
    """Public form page for participants to fill"""
    config = load_config()
    course = None
    for c in config['courses']:
        if c['id'] == course_id:
            course = c
            break
    
    if not course:
        return "Course not found. The link may be invalid or expired.", 404
    
    form = config['forms'].get(course['form_id'])
    if not form:
        return "Form not found", 404
    
    return render_template('form.html', form=form, course=course)


@app.route('/api/submit/<course_id>', methods=['POST'])
def submit_form(course_id):
    """Submit form response"""
    config = load_config()
    
    course = None
    for c in config['courses']:
        if c['id'] == course_id:
            course = c
            break
    
    if not course:
        return jsonify({'error': 'Course not found'}), 404
    
    data = request.json
    save_response(course['form_id'], course_id, data)
    
    return jsonify({'success': True, 'message': 'Thank you for your feedback!'})


if __name__ == '__main__':
    print("Checking for expired course links...")
    clean_expired_courses()
    
    config = load_config()
    for form_id in config['forms']:
        init_excel(form_id)
    
    app.run(debug=True, host='0.0.0.0', port=5000)
