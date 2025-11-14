import os
import uuid 
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from flask import Flask, render_template, request, redirect, url_for, session, jsonify, abort
from werkzeug.utils import secure_filename
from dotenv import load_dotenv
from agent import run_agent_graph

load_dotenv()

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'Uploads' 
app.config['SECRET_KEY'] = os.urandom(24) 
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

def get_db_admin_connection():
    """Connects to the main 'postgres' database to perform admin tasks."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            user=os.getenv("DB_ADMIN_USER"),
            password=os.getenv("DB_ADMIN_PASS"),
            dbname=os.getenv("DB_ADMIN_DBNAME")
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn
    except Exception as e:
        print(f"Error connecting to admin DB: {e}")
        return None

def create_db_from_sql(sql_filepath):
    """
    Creates a new, unique database and executes a .sql script on it.
    Returns the new database name if successful, else None.
    """
    admin_conn = get_db_admin_connection()  
    if not admin_conn:
        return None
    
    cursor = admin_conn.cursor() 
    new_db_name = f"user_db_{uuid.uuid4().hex[:10]}" 
    try:
        cursor.execute(f"CREATE DATABASE {new_db_name};")
        print(f"Successfully created database: {new_db_name}")
        
    except Exception as e:
        print(f"Error creating new database: {e}")
        cursor.close()
        admin_conn.close()
        return None
    finally:
        cursor.close()
        admin_conn.close()

    try:
        new_db_conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            user=os.getenv("DB_ADMIN_USER"),
            password=os.getenv("DB_ADMIN_PASS"),
            dbname=new_db_name
        )
        new_cursor = new_db_conn.cursor()
        with open(sql_filepath, 'r') as f:
            sql_script = f.read()
            new_cursor.execute(sql_script)
        
        new_db_conn.commit()
        print(f"Successfully populated database: {new_db_name}")
        
        return new_db_name
        
    except Exception as e:
        print(f"Error populating new database: {e}")
        return None
    finally:
        if 'new_cursor' in locals():
            new_cursor.close()
        if 'new_db_conn' in locals():
            new_db_conn.close()


@app.route('/')
def index():
    """Renders the main page with upload form and chat."""    
    db_name = session.get('db_name')
    return render_template('index.html', db_name=db_name)

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handles the .sql file upload."""
    if 'file' not in request.files:
        return "No file part", 400
    
    file = request.files['file']
    if file.filename == '':
        return "No selected file", 400
    
    if file and file.filename.endswith('.sql'):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath
        print(f"Attempting to create database from: {filepath}")
        new_db_name = create_db_from_sql(filepath)

        if new_db_name:
            session['db_name'] = new_db_name
            session['db_uri'] = (
                f"postgresql+psycopg2://{os.getenv('DB_ADMIN_USER')}:"
                f"{os.getenv('DB_ADMIN_PASS')}@"
                f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{new_db_name}"
            )
            print(f"Session set for database: {session['db_name']}")
        else:
            return "Error creating database from SQL file.", 500

        return redirect(url_for('index'))
    
    return "Invalid file type. Please upload a .sql file.", 400


@app.route('/chat', methods=['POST'])
def chat():
    """Handles chat messages from the user."""
    user_question = request.json.get('message')
    db_uri = session.get('db_uri')

    if not user_question:
        return jsonify({"error": "No message provided"}), 400
    
    if not db_uri:
        return jsonify({"error": "No database uploaded. Please upload a .sql file first."}), 400

    print(f"Running agent on: {session.get('db_name')} with question: {user_question}")
    
    try:
        
        result = run_agent_graph(user_question, db_uri)
        return jsonify({"answer": result})
    
    except Exception as e:
        print(f"Error during agent execution: {e}")
        return jsonify({"error": "An error occurred while processing your request."}), 500

@app.route('/clear', methods=['GET'])
def clear_session():
    """Clears the session to 'log out' and upload a new DB."""
    session.clear()
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True, port=5000)
