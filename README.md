# AKC Feedback System (AKC_FBS)

A web-based course feedback and evaluation platform built with Flask and Microsoft SQL Server. It lets participants submit feedback forms via QR code after completing a training course, and gives administrators a dashboard to manage forms, generate QR codes, and analyze responses.

---

## What it does

- **QR code–driven feedback** – Admins generate a unique QR code per course session. Participants scan it, enter their ID number, and fill in the evaluation form.
- **Multi-form, multi-language support** – Each form can be configured independently. Language variants (English, Chinese, Thai, Spanish, French, German, Japanese, Korean) can be published for the same form.
- **Dynamic form builder** – Admins can add, edit, or remove sections and questions directly from the dashboard without touching any code. Section types include rating scales, instructor/assessor ratings, free-text questions, multiple-choice, and yes/no.
- **Duplicate-submission protection** – A participant's ID number is checked against existing records before the form is shown; re-submissions are blocked.
- **Low-rating alerts** – Any response with a rating of 2 or below, or free-text containing negative sentiment keywords, is flagged automatically and surfaced in a dedicated alert view for admin follow-up.
- **Data analysis tab** – Charts and summary statistics for each course session, powered by Chart.js.
- **Two-database architecture** – Participant and course records live in `AKC_NAV` (the existing business system). Feedback responses are stored in a separate `AKC_FBS` database, keeping them cleanly isolated.

---

## Tech stack

| Layer | Technology |
|---|---|
| Backend | Python 3 / Flask 3.0 |
| Database | Microsoft SQL Server (via `pyodbc`) |
| Frontend | Jinja2 templates, vanilla JS, Chart.js |
| Deployment | IIS + `wfastcgi` (Windows Server) |
| QR generation | `qrcode[pil]` |
| Session management | `flask-session` |

---

## Project layout

```
AKC_FBS/
├── app.py               # Flask application – routes, business logic
├── db.py                # All database operations (two connections: AKC_NAV + AKC_FBS)
├── wsgi.py              # WSGI entry point for IIS / wfastcgi
├── web.config           # IIS FastCGI configuration
├── create_tables.sql    # SQL to initialise the AKC_FBS schema
├── requirements.txt     # Python dependencies
├── config.json          # Seed configuration (courses list)
├── static/
│   ├── logo.png
│   └── style.css
├── templates/
│   ├── admin.html         # Admin dashboard
│   ├── admin_login.html   # Admin login page
│   ├── form.html          # Participant feedback form
│   ├── scan.html          # QR scan / ID entry page
│   ├── student_login.html # Participant login
│   └── low_ratings.html   # Low-rating alert review page
├── data/                  # Runtime JSON files (alerts, lock files)
└── logs/                  # Application logs (session_debug.log)
```

---

## Prerequisites

- Python 3.10 or newer
- Microsoft SQL Server (2016+) with two databases: `AKC_NAV` and `AKC_FBS`
- ODBC Driver 18 for SQL Server (falls back to the legacy `SQL Server` driver automatically)
- IIS with the `wfastcgi` module installed, if deploying to Windows Server

---

## Setup

### 1. Clone and install dependencies

```bash
git clone <repo-url>
cd AKC_FBS
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### 2. Create the database schema

Run `create_tables.sql` against your `AKC_FBS` database:

```sql
-- In SSMS or sqlcmd, with AKC_FBS as the active database:
:r create_tables.sql
```

This creates `AdminUsers`, `FBS_Forms`, `FBS_Courses`, and `FBS_Responses`. Per-form response tables are created automatically at runtime.

### 3. Configure environment variables

Create a `.env` file in the project root:

```env
SECRET_KEY=<a-long-random-string>

# Admin credentials (fallback login when DB is unavailable)
ADMIN_ACCOUNT=admin
ADMIN_PASSWORD=<your-password>

# Database connection
DB_SERVER=<sql-server-hostname-or-ip>
DB_USERNAME=<sql-login>
DB_PASSWORD=<sql-password>
DB_NAV_DATABASE=AKC_NAV    # optional, defaults to AKC_NAV
DB_FBS_DATABASE=AKC_FBS    # optional, defaults to AKC_FBS
```

> **Note:** The app will refuse to start if `SECRET_KEY`, `ADMIN_ACCOUNT`, `ADMIN_PASSWORD`, `DB_SERVER`, `DB_USERNAME`, or `DB_PASSWORD` are missing.

### 4. Run locally

```bash
flask run
```

The app starts on `http://127.0.0.1:5000`. Navigate to `/login` to access the admin dashboard.

---

## Deploying to IIS

1. Copy the project folder to `C:\inetpub\wwwroot\AKC_FBS` (or wherever your site root is).
2. Install `wfastcgi` and enable FastCGI in IIS.
3. Update the paths in `web.config` to match your Python installation and project directory.
4. Set the environment variables listed above on the IIS application pool, or place a `.env` file in the project root.
5. Grant the application pool identity read/write access to the `data/` and `logs/` directories.

---

## Admin dashboard

Log in at `/login`. The dashboard has five tabs:

| Tab | Purpose |
|---|---|
| **Generate QR Code** | Create a new course session and download the QR code |
| **View Participants** | Browse participants and check submission status |
| **Edit Form Questions** | Add/remove sections and questions for any form |
| **Generated QR Codes** | Manage active and past QR sessions |
| **Data Analysis** | View charts and statistics for a chosen course |

The **Low Rating Feedback** link in the header opens the alert review page, where any response that triggered a low-rating or negative-sentiment flag can be actioned.

---

## Participant flow

1. Participant scans the QR code with their phone.
2. They enter the class code and their ID number on `/scan`.
3. The system looks up their name in `AKC_NAV` and checks they have not already submitted.
4. They fill in the feedback form and submit.
5. If multiple sessions are active for the same class code (e.g. both a trainer and an assessor evaluation), they are shown a choice screen first.

---

## Environment variables reference

| Variable | Required | Description |
|---|---|---|
| `SECRET_KEY` | ✅ | Flask session secret key |
| `ADMIN_ACCOUNT` | ✅ | Fallback admin username |
| `ADMIN_PASSWORD` | ✅ | Fallback admin password |
| `DB_SERVER` | ✅ | SQL Server host |
| `DB_USERNAME` | ✅ | SQL Server login |
| `DB_PASSWORD` | ✅ | SQL Server password |
| `DB_NAV_DATABASE` | ❌ | Source DB name (default: `AKC_NAV`) |
| `DB_FBS_DATABASE` | ❌ | Feedback DB name (default: `AKC_FBS`) |

---

## Logging

All application events are written to `logs/session_debug.log` and to the console. The log covers session activity, negative-feedback detection, database operations, and alert creation. The log file is created automatically on first run.

---

## License

Internal project — Absolute Kinetics Consultancy.
