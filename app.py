import html
import os
import random
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, quote, urlparse

import psycopg


FIRST_NAMES = [
    "Ava", "Noah", "Mia", "Liam", "Ella", "Ethan", "Sofia", "Leo", "Nora", "Jack",
    "Ivy", "Lucas", "Chloe", "Mason", "Grace", "Aria", "Owen", "Harper", "Zoe", "Ryan",
]
LAST_NAMES = [
    "Andersson", "Berg", "Lind", "Holm", "Svensson", "Larsson", "Karlsson", "Dahl",
    "Eriksson", "Nyberg", "Miller", "Davis", "Nguyen", "Patel", "Kim", "Garcia",
    "Brown", "Wilson", "Clark", "Lopez",
]
DEPARTMENTS = [
    "Engineering", "Data", "Finance", "HR", "Marketing", "Sales", "Operations", "Customer Success",
]
ROLES = ["Analyst", "Specialist", "Coordinator", "Manager", "Engineer", "Lead"]
SKILLS = [
    "Python", "SQL", "Spark", "Databricks", "Power BI", "Tableau", "Project Management",
    "Stakeholder Communication", "Machine Learning", "Data Modeling", "Financial Analysis",
    "Presentation", "Customer Discovery", "Forecasting",
]
TRAINING_TOPICS = [
    "Advanced SQL Optimization", "Databricks Lakehouse Fundamentals", "Machine Learning Essentials",
    "Data Storytelling", "Leadership for New Managers", "Agile Delivery", "Cloud Cost Optimization",
    "Data Governance and Compliance", "Strategic Communication", "Time and Priority Management",
]
COURSE_LIBRARY = [
    "Course: Databricks for Data Professionals",
    "Course: SQL Performance Tuning Bootcamp",
    "Course: Practical Spark Pipelines",
    "Course: BI Dashboards That Drive Action",
    "Course: Intro to ML in Production",
    "Course: Team Leadership Accelerator",
    "Course: Data Governance in Practice",
    "Course: Effective Executive Communication",
    "Course: Productive Project Planning",
    "Course: Forecasting and Scenario Modeling",
]

TABLE_FQN = os.environ.get("APP_TABLE_FQN", "public.tallent_training_register")
SCHEMA_INITIALIZED = False


def get_connection():
    db_url = os.environ.get("DATABASE_URL", "").strip()
    if not db_url:
        raise RuntimeError("Missing DATABASE_URL. This app runs in Neon/Postgres mode only.")
    return psycopg.connect(db_url)


def build_training_register(employee_count=100):
    rng = random.Random()
    employees = []
    for idx in range(1, employee_count + 1):
        employees.append(
            {
                "employee_id": f"EMP-{idx:03d}",
                "name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
                "department": rng.choice(DEPARTMENTS),
                "role": rng.choice(ROLES),
                "years_experience": rng.randint(0, 12),
                "skills": ", ".join(rng.sample(SKILLS, k=rng.randint(3, 6))),
                "training_needs": ", ".join(rng.sample(TRAINING_TOPICS, k=rng.randint(2, 4))),
                "recommended_courses": ", ".join(rng.sample(COURSE_LIBRARY, k=rng.randint(2, 4))),
                "priority": rng.choice(["High", "Medium", "Low"]),
                "target_months": rng.choice([1, 2, 3, 6, 9]),
            }
        )
    return employees


def sql_quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def ensure_schema_and_seed(conn):
    global SCHEMA_INITIALIZED
    if SCHEMA_INITIALIZED:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.tallent_training_register (
                employee_id TEXT PRIMARY KEY,
                full_name TEXT NOT NULL,
                department TEXT NOT NULL,
                role_title TEXT NOT NULL,
                years_experience INTEGER NOT NULL,
                skills TEXT NOT NULL,
                training_needs TEXT NOT NULL,
                recommended_courses TEXT NOT NULL,
                priority TEXT NOT NULL,
                target_months INTEGER NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        cur.execute(f"SELECT COUNT(*) FROM {TABLE_FQN}")
        count = cur.fetchone()[0]
        if count == 0:
            seed_rows = build_training_register(100)
            values_sql = []
            for row in seed_rows:
                tuple_values = ", ".join(
                    [
                        sql_quote(row["employee_id"]),
                        sql_quote(row["name"]),
                        sql_quote(row["department"]),
                        sql_quote(row["role"]),
                        str(row["years_experience"]),
                        sql_quote(row["skills"]),
                        sql_quote(row["training_needs"]),
                        sql_quote(row["recommended_courses"]),
                        sql_quote(row["priority"]),
                        str(row["target_months"]),
                        "NOW()",
                    ]
                )
                values_sql.append(f"({tuple_values})")
            cur.execute(
                f"""
                INSERT INTO {TABLE_FQN} (
                    employee_id, full_name, department, role_title, years_experience,
                    skills, training_needs, recommended_courses, priority, target_months, created_at
                ) VALUES
                """
                + ",\n".join(values_sql)
            )
    conn.commit()
    SCHEMA_INITIALIZED = True


def fetch_dashboard_data(conn):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                COUNT(*) AS total_employees,
                SUM(CASE WHEN priority = 'High' THEN 1 ELSE 0 END) AS high_priority,
                AVG(array_length(string_to_array(skills, ', '), 1)) AS avg_skills,
                AVG(array_length(string_to_array(training_needs, ', '), 1)) AS avg_needs
            FROM {TABLE_FQN}
            """
        )
        stats = cur.fetchone()
        cur.execute(
            f"""
            SELECT department, COUNT(*) AS employee_count
            FROM {TABLE_FQN}
            GROUP BY department
            ORDER BY employee_count DESC, department ASC
            """
        )
        department_rows = cur.fetchall()
        cur.execute(
            f"""
            SELECT employee_id, full_name, department, role_title, years_experience,
                   skills, training_needs, recommended_courses, priority, target_months
            FROM {TABLE_FQN}
            ORDER BY employee_id
            """
        )
        employee_rows = cur.fetchall()
    return stats, department_rows, employee_rows


def insert_employee(conn, form_values):
    required_fields = [
        "employee_id", "full_name", "department", "role_title", "years_experience",
        "skills", "training_needs", "recommended_courses", "priority", "target_months",
    ]
    missing = [field for field in required_fields if not form_values.get(field, "").strip()]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")

    employee_id = form_values["employee_id"].strip().upper()
    full_name = form_values["full_name"].strip()
    department = form_values["department"].strip()
    role_title = form_values["role_title"].strip()
    skills = form_values["skills"].strip()
    training_needs = form_values["training_needs"].strip()
    recommended_courses = form_values["recommended_courses"].strip()
    priority = form_values["priority"].strip().title()

    if priority not in {"High", "Medium", "Low"}:
        raise ValueError("Priority must be High, Medium, or Low.")

    try:
        years_experience = int(form_values["years_experience"].strip())
        target_months = int(form_values["target_months"].strip())
    except ValueError as exc:
        raise ValueError("Years experience and target months must be integers.") from exc

    if years_experience < 0 or years_experience > 60:
        raise ValueError("Years experience must be between 0 and 60.")
    if target_months < 1 or target_months > 36:
        raise ValueError("Target months must be between 1 and 36.")

    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {TABLE_FQN} WHERE employee_id = %s", (employee_id,))
        exists = cur.fetchone()[0]
        if exists:
            raise ValueError(f"Employee ID {employee_id} already exists.")

        cur.execute(
            f"""
            INSERT INTO {TABLE_FQN} (
                employee_id, full_name, department, role_title, years_experience,
                skills, training_needs, recommended_courses, priority, target_months, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """,
            (
                employee_id,
                full_name,
                department,
                role_title,
                years_experience,
                skills,
                training_needs,
                recommended_courses,
                priority,
                target_months,
            ),
        )
    conn.commit()


def render_summary_tiles(stats):
    return (
        "<div class='tiles'>"
        f"<div class='tile'><h3>{int(stats[0])}</h3><p>Total Employees</p></div>"
        f"<div class='tile'><h3>{int(stats[1] or 0)}</h3><p>High Priority Development</p></div>"
        f"<div class='tile'><h3>{float(stats[2] or 0):.1f}</h3><p>Avg Skills / Employee</p></div>"
        f"<div class='tile'><h3>{float(stats[3] or 0):.1f}</h3><p>Avg Training Needs / Employee</p></div>"
        "</div>"
    )


def render_department_chart(department_rows):
    if not department_rows:
        return "<p>No department data yet.</p>"
    max_count = max(row[1] for row in department_rows) or 1
    rows = []
    for dept, count in department_rows:
        width = max(8, int((count / max_count) * 100))
        rows.append(
            "<div class='chart-row'>"
            f"<div class='label'>{html.escape(dept)}</div>"
            f"<div class='bar-wrap'><div class='bar' style='width:{width}%;'></div></div>"
            f"<div class='count'>{count}</div>"
            "</div>"
        )
    return "\n".join(rows)


def render_table(employee_rows):
    rows = []
    for employee in employee_rows:
        priority = employee[8]
        if priority == "High":
            priority_html = "<span class='badge-high'>High</span>"
        elif priority == "Medium":
            priority_html = "<span class='badge-medium'>Medium</span>"
        else:
            priority_html = "<span class='badge-low'>Low</span>"
        rows.append(
            "<tr>"
            f"<td>{employee[0]}</td>"
            f"<td>{html.escape(employee[1])}</td>"
            f"<td>{html.escape(employee[2])}</td>"
            f"<td>{html.escape(employee[3])}</td>"
            f"<td>{employee[4]}</td>"
            f"<td>{html.escape(employee[5])}</td>"
            f"<td>{html.escape(employee[6])}</td>"
            f"<td>{html.escape(employee[7])}</td>"
            f"<td>{priority_html}</td>"
            f"<td>{employee[9]} months</td>"
            "</tr>"
        )
    return "\n".join(rows)


def render_page(saved=False, error_message=""):
    try:
        with get_connection() as conn:
            ensure_schema_and_seed(conn)
            stats, department_rows, employee_rows = fetch_dashboard_data(conn)
        summary_tiles = render_summary_tiles(stats)
        chart_html = render_department_chart(department_rows)
        table_html = render_table(employee_rows)
        warning_html = ""
    except Exception as err:
        summary_tiles = ""
        chart_html = ""
        table_html = ""
        warning_html = (
            "<div class='error'>"
            "<strong>Database connection error:</strong> "
            f"{html.escape(str(err))}<br/><br/>"
            "This local copy expects a valid <code>DATABASE_URL</code> (Neon/Postgres)."
            "</div>"
        )

    flash_html = ""
    if saved:
        flash_html = (
            "<div class='success'>"
            "<strong>Saved:</strong> Employee added to the training register."
            "</div>"
        )
    elif error_message:
        flash_html = (
            "<div class='error'>"
            f"<strong>Could not save employee:</strong> {html.escape(error_message)}"
            "</div>"
        )

    return f"""<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Tallent Management Portal</title>
    <style>
      body {{
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        margin: 0;
        background: #f1f5f9;
        color: #0f172a;
      }}
      main {{
        max-width: 1280px;
        margin: 32px auto;
        background: white;
        border-radius: 12px;
        padding: 24px;
        box-shadow: 0 12px 28px rgba(2, 6, 23, 0.08);
      }}
      h1 {{
        margin: 0 0 8px 0;
      }}
      p.lead {{
        margin: 0 0 18px 0;
        color: #334155;
      }}
      .tiles {{
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 12px;
        margin-bottom: 20px;
      }}
      .tile {{
        background: #0f172a;
        color: white;
        border-radius: 10px;
        padding: 14px;
      }}
      .tile h3 {{
        margin: 0;
        font-size: 1.4rem;
      }}
      .tile p {{
        margin: 4px 0 0 0;
        color: #cbd5e1;
        font-size: 0.9rem;
      }}
      .section {{
        margin-top: 20px;
      }}
      .chart-row {{
        display: grid;
        grid-template-columns: 220px 1fr 64px;
        gap: 10px;
        align-items: center;
        margin-bottom: 10px;
      }}
      .label {{
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }}
      .bar-wrap {{
        background: #e2e8f0;
        border-radius: 999px;
        height: 16px;
      }}
      .bar {{
        background: linear-gradient(90deg, #22c55e, #16a34a);
        height: 16px;
        border-radius: 999px;
      }}
      .count {{
        text-align: right;
        font-variant-numeric: tabular-nums;
      }}
      .table-wrap {{
        overflow-x: auto;
        border: 1px solid #e2e8f0;
        border-radius: 8px;
      }}
      .success {{
        background: #ecfdf3;
        border: 1px solid #86efac;
        color: #166534;
        padding: 10px 12px;
        border-radius: 8px;
        margin-bottom: 14px;
      }}
      .error {{
        background: #fff7ed;
        border: 1px solid #fdba74;
        color: #9a3412;
        padding: 10px 12px;
        border-radius: 8px;
        margin-bottom: 14px;
      }}
      .form-grid {{
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 10px;
      }}
      .form-grid .full {{
        grid-column: 1 / -1;
      }}
      label {{
        display: block;
        font-size: 0.84rem;
        color: #334155;
        margin-bottom: 4px;
      }}
      input, select {{
        width: 100%;
        box-sizing: border-box;
        border: 1px solid #cbd5e1;
        border-radius: 8px;
        padding: 8px 10px;
        font-size: 0.9rem;
      }}
      button {{
        border: 0;
        border-radius: 8px;
        background: #0f172a;
        color: white;
        padding: 10px 14px;
        font-weight: 600;
        cursor: pointer;
      }}
      table {{
        width: 100%;
        border-collapse: collapse;
        min-width: 1200px;
      }}
      th, td {{
        border-bottom: 1px solid #e2e8f0;
        padding: 10px 8px;
        text-align: left;
        vertical-align: top;
        font-size: 0.88rem;
      }}
      th {{
        position: sticky;
        top: 0;
        background: #f8fafc;
        z-index: 1;
      }}
      tr:nth-child(even) {{
        background: #fcfdff;
      }}
      .badge-high {{
        color: #991b1b;
        background: #fee2e2;
        padding: 2px 8px;
        border-radius: 999px;
      }}
      .badge-medium {{
        color: #92400e;
        background: #fef3c7;
        padding: 2px 8px;
        border-radius: 999px;
      }}
      .badge-low {{
        color: #065f46;
        background: #d1fae5;
        padding: 2px 8px;
        border-radius: 999px;
      }}
      code {{
        background: #f1f5f9;
        padding: 0.1rem 0.3rem;
        border-radius: 6px;
      }}
    </style>
  </head>
  <body>
    <main>
      <h1>Tallent Management Portal</h1>
      <p class="lead">
        Training register with 100 randomly generated employees, their current skills,
        training needs, and recommended development courses, stored in Neon/Postgres.
      </p>
      {warning_html}
      {flash_html}
      {summary_tiles}
      <div class="section">
        <h2>Add Employee</h2>
        <form method="post" action="/add-employee">
          <div class="form-grid">
            <div>
              <label for="employee_id">Employee ID</label>
              <input id="employee_id" name="employee_id" placeholder="EMP-101" required />
            </div>
            <div>
              <label for="full_name">Full Name</label>
              <input id="full_name" name="full_name" placeholder="Alex Johnson" required />
            </div>
            <div>
              <label for="department">Department</label>
              <input id="department" name="department" placeholder="Engineering" required />
            </div>
            <div>
              <label for="role_title">Role</label>
              <input id="role_title" name="role_title" placeholder="Data Engineer" required />
            </div>
            <div>
              <label for="years_experience">Years Experience</label>
              <input id="years_experience" name="years_experience" type="number" min="0" max="60" required />
            </div>
            <div>
              <label for="priority">Priority</label>
              <select id="priority" name="priority" required>
                <option value="High">High</option>
                <option value="Medium" selected>Medium</option>
                <option value="Low">Low</option>
              </select>
            </div>
            <div class="full">
              <label for="skills">Skills (comma-separated)</label>
              <input id="skills" name="skills" placeholder="Python, SQL, Databricks" required />
            </div>
            <div class="full">
              <label for="training_needs">Training Needs (comma-separated)</label>
              <input id="training_needs" name="training_needs" placeholder="Advanced SQL Optimization, Leadership for New Managers" required />
            </div>
            <div class="full">
              <label for="recommended_courses">Recommended Courses (comma-separated)</label>
              <input id="recommended_courses" name="recommended_courses" placeholder="Course: SQL Performance Tuning Bootcamp, Course: Team Leadership Accelerator" required />
            </div>
            <div>
              <label for="target_months">Target Completion (months)</label>
              <input id="target_months" name="target_months" type="number" min="1" max="36" required />
            </div>
            <div style="display:flex; align-items:flex-end;">
              <button type="submit">Add Employee</button>
            </div>
          </div>
        </form>
      </div>
      <div class="section">
        <h2>Employees by Department</h2>
        {chart_html}
      </div>
      <div class="section">
        <h2>Employee Training Register</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Name</th>
                <th>Department</th>
                <th>Role</th>
                <th>Years</th>
                <th>Skills</th>
                <th>Training Needs</th>
                <th>Recommended Courses</th>
                <th>Priority</th>
                <th>Target Completion</th>
              </tr>
            </thead>
            <tbody>
              {table_html}
            </tbody>
          </table>
        </div>
      </div>
      <p style="margin-top:16px; color:#64748b;">
        Data persists in table <code>{html.escape(TABLE_FQN)}</code>.
      </p>
    </main>
  </body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        saved = params.get("saved", ["0"])[0] == "1"
        error_message = params.get("error", [""])[0]
        body = render_page(saved=saved, error_message=error_message).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def handle_add_employee(self):
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(content_length).decode("utf-8")
            form_data = {k: v[0] for k, v in parse_qs(raw_body).items()}
            with get_connection() as conn:
                ensure_schema_and_seed(conn)
                insert_employee(conn, form_data)
            location = "/?saved=1"
        except Exception as err:
            location = f"/?error={quote(str(err))}"

        self.send_response(303)
        self.send_header("Location", location)
        self.end_headers()

    def do_POST(self):
        if self.path == "/add-employee":
            self.handle_add_employee()
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, fmt, *args):
        print("[http]", fmt % args)


if __name__ == "__main__":
    port = int(
        os.environ.get("PORT")
        or os.environ.get("DATABRICKS_APP_PORT")
        or "8000"
    )
    server = HTTPServer(("0.0.0.0", port), Handler)
    print(f"Serving Tallent Management Portal on 0.0.0.0:{port}")
    server.serve_forever()
