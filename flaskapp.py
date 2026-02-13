from flask import Flask, render_template, request, redirect, url_for, session
from werkzeug.utils import secure_filename
import boto3
import os
import sqlite3

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
S3_BUCKET = os.environ.get("S3_BUCKET")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_PREFIX = os.environ.get("S3_PREFIX", "uploads")
S3_URL_EXPIRES = int(os.environ.get("S3_URL_EXPIRES", "300"))

DB_PATH = os.path.join(BASE_DIR, "users.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute(
        """CREATE TABLE IF NOT EXISTS users
        (username TEXT, password TEXT, firstname TEXT, lastname TEXT, email TEXT,
         file_name TEXT, s3_key TEXT, s3_bucket TEXT, wordcount INTEGER)"""
    )
    conn.commit()

    columns = {row["name"] for row in c.execute("PRAGMA table_info(users)")}
    if "file_name" not in columns:
        c.execute("ALTER TABLE users ADD COLUMN file_name TEXT")
    if "s3_key" not in columns:
        c.execute("ALTER TABLE users ADD COLUMN s3_key TEXT")
    if "s3_bucket" not in columns:
        c.execute("ALTER TABLE users ADD COLUMN s3_bucket TEXT")
    if "wordcount" not in columns:
        c.execute("ALTER TABLE users ADD COLUMN wordcount INTEGER")
    conn.commit()
    conn.close()


init_db()

@app.route('/')
def index():
    return render_template('home.html')


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'GET':
        return render_template('register.html')

    username = request.form['username'].strip()
    password = request.form['password']
    confirm_password = request.form['confirm_password']
    firstname = request.form['first_name'].strip()
    lastname = request.form['last_name'].strip()
    email = request.form['email'].strip()

    if password != confirm_password:
        return render_template('register.html', error="Passwords do not match.")

    file = request.files.get('upload')
    file_name = None
    s3_key = None
    s3_bucket = None
    wordcount = None

    if file and file.filename:
        if not S3_BUCKET:
            return render_template('register.html', error="S3_BUCKET is not configured.")

        safe_name = secure_filename(file.filename)
        content = file.read().decode("utf-8", errors="ignore")
        wordcount = len(content.split())
        file.seek(0)

        s3_key = f"{S3_PREFIX}/{username}/{safe_name}"
        s3_client = boto3.client("s3", region_name=AWS_REGION)
        s3_client.upload_fileobj(file, S3_BUCKET, s3_key)

        file_name = safe_name
        s3_bucket = S3_BUCKET

    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT username FROM users WHERE username=?", (username,))
    existing = c.fetchone()
    if existing:
        conn.close()
        return render_template('register.html', error="Username already exists.")

    c.execute(
        "INSERT INTO users (username, password, firstname, lastname, email, file_name, s3_key, s3_bucket, wordcount) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (username, password, firstname, lastname, email, file_name, s3_key, s3_bucket, wordcount)
    )
    conn.commit()
    conn.close()

    return redirect(url_for('index'))


@app.route('/signin', methods=['POST'])
def signin():
    username = request.form['username'].strip()
    password = request.form['password']

    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE username=?", (username,))
    user = c.fetchone()

    if not user or user["password"] != password:
        conn.close()
        return render_template('home.html', error="Invalid username or password.")

    conn.close()

    session['username'] = username
    return redirect(url_for('profile'))

@app.route('/profile')
def profile():
    username = session.get('username')
    if not username:
        return redirect(url_for('index'))

    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE username=?", (username,))
    user = c.fetchone()
    conn.close()

    if not user:
        session.pop('username', None)
        return redirect(url_for('index'))

    wordcount_value = "N/A"
    if user["wordcount"] is not None:
        wordcount_value = f"{user['wordcount']} words"

    user_view = {
        "username": user["username"],
        "first_name": user["firstname"],
        "last_name": user["lastname"],
        "email": user["email"],
    }
    presigned_url = None
    if user["s3_key"] and user["s3_bucket"]:
        s3_client = boto3.client("s3", region_name=AWS_REGION)
        presigned_url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": user["s3_bucket"], "Key": user["s3_key"]},
            ExpiresIn=S3_URL_EXPIRES,
        )

    return render_template(
        'profile.html',
        user=user_view,
        wordcount=wordcount_value,
        presigned_url=presigned_url
    )


@app.route('/signout')
def signout():
    session.pop('username', None)
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)
