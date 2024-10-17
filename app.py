from flask import Flask, request, render_template, send_from_directory, Response, stream_with_context, jsonify
import os
import pandas as pd
from werkzeug.utils import secure_filename
from processor import process_excel, insert_data_to_sql
from dotenv import load_dotenv
import pyodbc
import json
from itertools import islice
from datetime import datetime


load_dotenv()

app = Flask(__name__)

server = os.getenv('SERVER')
database = os.getenv('DATABASE')
username = os.getenv('USER_NAME')
password = os.getenv('PASSWORD')


UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

@app.route('/')
def index():
    return render_template('upload.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    if 'files' not in request.files:
        return 'No files part'
    
    files = request.files.getlist('files')
    file_sheets = {}

    for file in files:
        if file.filename == '':
            return 'No selected file'

        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        try:
            xl = pd.ExcelFile(filepath)
            file_sheets[filename] = xl.sheet_names
        except Exception as e:
            return f"Error reading {filename}: {str(e)}"
    

    return render_template('sheet_selection.html', file_sheets=file_sheets)

def get_db_connection():
    return pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')

def batch(iterable, batch_size):
    iterable = iter(iterable)
    while True:
        chunk = list(islice(iterable, batch_size))
        if not chunk:
            break
        yield chunk

@app.route('/process', methods=['POST'])
def process_sheets():
    selected_sheets = request.form.to_dict(flat=False)

    @stream_with_context
    def generate():
        consolidated_data = []
        conn = get_db_connection()
        batch_size = 10  # Number of files to process per batch
        batch_count = 0  # Keep track of batch numbers

        for file_batch in batch(selected_sheets.items(), batch_size):
            batch_count += 1  # Increment batch counter
            for file, sheets in file_batch:
                file = file.split('[')[0]
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], file)

                # Check if the file exists and is not empty
                if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
                    yield json.dumps({'status': 'error', 'file': file, 'error': 'File is empty or missing'}).encode('utf-8') + b'\n\n'
                    continue

                for sheet in sheets:
                    try:
                        # Notify about the start of processing
                        yield json.dumps({'status': 'processing', 'file': file, 'sheet': sheet}).encode('utf-8') + b'\n\n'

                        df = process_excel(filepath, sheet)
                        if df is None or df.empty or len(df) == 1:
                            yield json.dumps({'status': 'error', 'file': file, 'sheet': sheet, 'error': f'{file} sheet is empty'}).encode('utf-8') + b'\n\n'
                            continue

                        consolidated_data.append(df)

                        # Notify about completion
                        yield json.dumps({'status': 'done', 'file': file, 'sheet': sheet}).encode('utf-8') + b'\n\n'

                    except Exception as e:
                        yield json.dumps({'status': 'error', 'file': file, 'sheet': sheet, 'error': str(e)}).encode('utf-8') + b'\n\n'

            # After processing a batch, insert into the database
            if consolidated_data:
                try:
                    final_df = pd.concat(consolidated_data, ignore_index=True)
                    final_df = final_df.astype(str)
                    insert_data_to_sql(final_df, conn)

                    # Generate a safe filename for the batch
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    consolidated_file_path = os.path.join(app.config['UPLOAD_FOLDER'], f'consolidated_batch_{batch_count}_{timestamp}.xlsx')
                    final_df.to_excel(consolidated_file_path, index=False)

                    yield json.dumps({'status': 'done', 'file': f'consolidated_batch_{batch_count}', 'message': 'Batch data has been inserted into the database.'}).encode('utf-8') + b'\n\n'

                except Exception as e:
                    yield json.dumps({'status': 'error', 'file': 'N/A', 'sheet': 'N/A', 'error': 'Database error: ' + str(e)}).encode('utf-8') + b'\n\n'

        conn.close()

    return Response(generate(), mimetype='application/json')

@app.route('/uploads/<filename>')
def download_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

if __name__ == '__main__':
    app.run(debug=True)
