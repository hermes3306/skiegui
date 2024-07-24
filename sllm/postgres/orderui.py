import sys
import os
import configparser
import random
from datetime import datetime, timedelta
import csv
from decimal import Decimal

# Database related imports
import psycopg2
from psycopg2.extras import execute_values
from neo4j import GraphDatabase

# Data manipulation
import pandas as pd

# PyQt6 imports
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QComboBox, QTableWidget, 
    QVBoxLayout, QHBoxLayout, QWidget, QTextEdit, 
    QLabel, QStatusBar, QMenuBar, QMenu, QTableWidgetItem,
    QHeaderView, QPushButton, QFileDialog, QMessageBox,
    QPlainTextEdit, QDialog, QSizePolicy
)
from PyQt6.QtGui import QAction, QColor, QBrush, QFont, QTextCharFormat, QSyntaxHighlighter, QPainter
from PyQt6.QtCore import Qt, QRegularExpression, QRect, QSize
from PyQt6.QtWidgets import QTabWidget


# Read database configuration
config = configparser.ConfigParser()
config.read('db.ini')

# PostgreSQL connection details
pg_host = config['postgresql']['host']
pg_port = config['postgresql']['port']
pg_database = config['postgresql']['database']
pg_user = config['postgresql']['user']
pg_password = config['postgresql']['password']

# Neo4j connection details
neo4j_url = config['neo4j']['url']
neo4j_user = config['neo4j']['user']
neo4j_password = config['neo4j']['password']

class LineNumberArea(QWidget):
    def __init__(self, editor):
        super().__init__(editor)
        self.editor = editor

    def sizeHint(self):
        return QSize(self.editor.line_number_area_width(), 0)

    def paintEvent(self, event):
        self.editor.lineNumberAreaPaintEvent(event)

class CsvViewerDialog(QDialog):
    def __init__(self, file_path):
        super().__init__()
        self.file_path = file_path
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle(f"CSV Viewer - {os.path.basename(self.file_path)}")
        self.setGeometry(200, 200, 1000, 700)

        layout = QVBoxLayout()

        # File info
        file_size = os.path.getsize(self.file_path)
        file_info = f"File: {os.path.basename(self.file_path)} | Size: {file_size} bytes"
        info_label = QLabel(file_info)
        layout.addWidget(info_label)

        # Text editor
        self.editor = QPlainTextEdit()
        self.editor.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        font = QFont("Courier New", 10)
        self.editor.setFont(font)
        layout.addWidget(self.editor)

        # Load content
        with open(self.file_path, 'r') as file:
            content = file.read()
        self.editor.setPlainText(content)

        # Apply syntax highlighting
        self.highlighter = CsvHighlighter(self.editor.document())

        self.editor.setReadOnly(True)

        self.setLayout(layout)

class CsvHighlighter(QSyntaxHighlighter):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.highlight_rules = []

        # Define a list of colors for columns
        self.column_colors = [
            QColor("#FFB3BA"),  # Light Pink
            QColor("#BAFFC9"),  # Light Green
            QColor("#BAE1FF"),  # Light Blue
            QColor("#FFFFBA"),  # Light Yellow
            QColor("#FFD8B3"),  # Light Orange
            QColor("#E0B3FF"),  # Light Purple
            QColor("#B3FFF6"),  # Light Cyan
            QColor("#FFC8B3"),  # Light Coral
        ]

        # Highlight header (first line)
        header_format = QTextCharFormat()
        header_format.setFontWeight(QFont.Weight.Bold)
        header_format.setBackground(QColor("#E0E0E0"))  # Light Gray background
        self.highlight_rules.append(("^.+$", header_format, 0))

    def highlightBlock(self, text):
        # First, apply header highlighting
        for pattern, format, _ in self.highlight_rules:
            expression = QRegularExpression(pattern)
            match = expression.match(text)
            if match.hasMatch():
                start = match.capturedStart()
                length = match.capturedLength()
                self.setFormat(start, length, format)

        # Then, apply column-based coloring
        if self.currentBlock().blockNumber() > 0:  # Skip the header row
            columns = text.split(',')
            start_index = 0
            for i, column in enumerate(columns):
                color = self.column_colors[i % len(self.column_colors)]
                format = QTextCharFormat()
                format.setBackground(color)
                self.setFormat(start_index, len(column), format)
                start_index += len(column) + 1  # +1 for the comma
                
class DatabaseViewer(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Database Viewer")
        self.setGeometry(100, 100, 1000, 600)

        self.conn = None
        self.cur = None
        self.config = None
        self.current_csv_file = None

        self.init_ui()
        self.connect_to_db()
        self.neo4j_driver = GraphDatabase.driver(neo4j_url, auth=(neo4j_user, neo4j_password))

    def init_ui(self):
        # Menu bar
        menubar = self.menuBar()
        file_menu = menubar.addMenu("File")
        
        exit_action = QAction("Exit", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # Main widget and layout
        main_widget = QWidget()
        main_layout = QVBoxLayout()

        # Create tab widget
        self.tab_widget = QTabWidget()
        main_layout.addWidget(self.tab_widget)

        # PostgreSQL tab
        pg_tab = QWidget()
        pg_layout = QVBoxLayout()
        self.setup_postgresql_ui(pg_layout)
        pg_tab.setLayout(pg_layout)
        self.tab_widget.addTab(pg_tab, "PostgreSQL")

        # MongoDB tab
        mongo_tab = QWidget()
        mongo_layout = QVBoxLayout()
        self.setup_mongodb_ui(mongo_layout)
        mongo_tab.setLayout(mongo_layout)
        self.tab_widget.addTab(mongo_tab, "MongoDB")

        # Neo4j tab
        neo4j_tab = QWidget()
        neo4j_layout = QVBoxLayout()
        self.setup_neo4j_ui(neo4j_layout)
        neo4j_tab.setLayout(neo4j_layout)
        self.tab_widget.addTab(neo4j_tab, "Neo4j")

        main_widget.setLayout(main_layout)
        self.setCentralWidget(main_widget)

        # Status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)

    def setup_postgresql_ui(self, layout):
        # Database info
        self.db_info_label = QLabel("Database: Not connected")
        layout.addWidget(self.db_info_label)

        # Table selection and CSV buttons
        table_select_layout = QHBoxLayout()
        table_select_layout.addWidget(QLabel("Select Table:"))
        self.table_combo = QComboBox()
        self.table_combo.currentTextChanged.connect(self.load_table_data)
        table_select_layout.addWidget(self.table_combo)
        
        self.download_csv_btn = QPushButton("Download CSV")
        self.download_csv_btn.clicked.connect(self.download_csv_clicked)
        table_select_layout.addWidget(self.download_csv_btn)

        self.view_csv_btn = QPushButton("View CSV")
        self.view_csv_btn.clicked.connect(self.view_csv)
        table_select_layout.addWidget(self.view_csv_btn)

        self.upload_csv_btn = QPushButton("Upload CSV")
        self.upload_csv_btn.clicked.connect(self.upload_csv)
        table_select_layout.addWidget(self.upload_csv_btn)

        layout.addLayout(table_select_layout)

        # Table view
        self.table_widget = QTableWidget()
        self.table_widget.setAlternatingRowColors(True)
        self.table_widget.setStyleSheet("""
            QTableWidget {
                alternate-background-color: #f0f0f0;
                background-color: white;
            }
            QHeaderView::section {
                background-color: #e0e0e0;
                padding: 4px;
                border: 1px solid #c0c0c0;
                font-weight: bold;
            }
        """)
        layout.addWidget(self.table_widget)

        # Log messages
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMaximumHeight(100)
        layout.addWidget(self.log_text)

    def setup_mongodb_ui(self, layout):
        # Placeholder for MongoDB UI
        layout.addWidget(QLabel("MongoDB functionality not implemented yet"))

    def setup_neo4j_ui(self, layout):
        # Cypher to upload CSV button
        self.cypher_upload_btn = QPushButton("Cypher to upload CSV")
        self.cypher_upload_btn.clicked.connect(self.upload_csv_to_neo4j)
        layout.addWidget(self.cypher_upload_btn)

        # Placeholder for additional Neo4j UI elements
        layout.addWidget(QLabel("Additional Neo4j functionality can be added here"))

    def connect_to_db(self):
        try:
            self.conn = psycopg2.connect(
                host=pg_host,
                port=pg_port,
                database=pg_database,
                user=pg_user,
                password=pg_password
            )
            self.cur = self.conn.cursor()

            self.db_info_label.setText(f"Database: {pg_database} on {pg_host}:{pg_port}\n"
                                       f"User: {pg_user} | Password: {'*' * len(pg_password)}")
            self.update_status("Connected to database", "green")

            self.log_message("Connected to database successfully")
            self.load_tables()
        except Exception as e:
            self.log_message(f"Error connecting to database: {str(e)}")
            self.update_status("Database connection failed", "red")

    def upload_csv(self):
        file_name, _ = QFileDialog.getOpenFileName(self, "Open CSV", "", "CSV Files (*.csv)")
        if file_name:
            table_name = os.path.splitext(os.path.basename(file_name))[0]
            try:
                # Read CSV file
                df = pd.read_csv(file_name)
                
                # Create table in PostgreSQL
                self.create_table_from_df(table_name, df)
                
                # Insert data into PostgreSQL
                self.insert_data_to_postgres(table_name, df)
                
                self.log_message(f"CSV file uploaded to PostgreSQL table: {table_name}")
                self.update_status(f"CSV uploaded: {table_name}", "#A0A0A0")
                
                # Refresh the table list
                self.load_tables()
                
                # Select the newly created table in the combo box
                index = self.table_combo.findText(table_name)
                if index >= 0:
                    self.table_combo.setCurrentIndex(index)
                
                # Load and display the data
                self.load_table_data(table_name)
                
            except Exception as e:
                self.conn.rollback()
                self.log_message(f"Error uploading CSV: {str(e)}")
                self.update_status("Error uploading CSV", "red")

    def create_table_from_df(self, table_name, df):
        # Generate CREATE TABLE statement
        columns = []
        for column, dtype in df.dtypes.items():
            if dtype == 'int64':
                col_type = 'INTEGER'
            elif dtype == 'float64':
                col_type = 'FLOAT'
            else:
                col_type = 'TEXT'
            columns.append(f'"{column}" {col_type}')
        
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
        
        self.cur.execute(create_table_query)
        self.conn.commit()

    def insert_data_to_postgres(self, table_name, df):
        # Insert data into the table
        columns = ', '.join(f'"{col}"' for col in df.columns)
        values = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        
        data = [tuple(row) for row in df.values]
        self.cur.executemany(insert_query, data)
        self.conn.commit()

    def load_tables(self):
        if self.cur:
            try:
                self.cur.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public'
                """)
                tables = [table[0] for table in self.cur.fetchall()]
                self.log_message(f"Available tables: {tables}")
                self.table_combo.clear()
                self.table_combo.addItems(tables)
            except Exception as e:
                self.log_message(f"Error loading tables: {str(e)}")

    def load_table_data(self, table_name):
        if self.cur and table_name:
            try:
                self.cur.execute(f"SELECT * FROM {table_name}")
                rows = self.cur.fetchall()
                
                # Get column names
                col_names = [desc[0] for desc in self.cur.description]
                
                # Set up the table
                self.table_widget.setRowCount(len(rows))
                self.table_widget.setColumnCount(len(col_names))
                self.table_widget.setHorizontalHeaderLabels(col_names)

                # Populate the table
                for i, row in enumerate(rows):
                    for j, value in enumerate(row):
                        item = QTableWidgetItem(str(value))
                        self.table_widget.setItem(i, j, item)

                # Adjust column widths
                self.table_widget.resizeColumnsToContents()

                self.log_message(f"Loaded data from table: {table_name}")
                self.update_status(f"Viewing table: {table_name}", "#A0A0A0")  # Light gray
            except Exception as e:
                self.log_message(f"Error loading table data: {str(e)}")
                self.update_status(f"Error loading table: {table_name}", "red")
                
    def download_csv_clicked(self):
        table_name = self.table_combo.currentText()
        self.log_message(f"Download CSV clicked. Selected table: '{table_name}'")
        if table_name:
            success = self.download_csv(table_name)
            if success:
                self.log_message(f"CSV for table '{table_name}' downloaded successfully")
            else:
                self.log_message(f"Failed to download CSV for table '{table_name}'")
        else:
            self.log_message("No table selected for CSV download")
            self.update_status("No table selected", "red")

    def download_csv(self, table_name):
        self.log_message(f"Attempting to download CSV for table: '{table_name}'")
        
        if not table_name:
            self.log_message("No table selected")
            self.update_status("No table selected", "red")
            return False

        file_name = f"{table_name}.csv"
        file_path = os.path.join(os.getcwd(), file_name)
        
        try:
            self.log_message(f"Executing query: SELECT * FROM {table_name}")
            self.cur.execute(f"SELECT * FROM {table_name}")
            
            self.log_message("Fetching results...")
            rows = self.cur.fetchall()
            
            self.log_message("Getting column names...")
            col_names = [desc[0] for desc in self.cur.description]

            self.log_message(f"Writing to file: {file_path}")
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                csv_writer.writerow(col_names)
                for row in rows:
                    csv_writer.writerow([str(cell) if cell is not None else '' for cell in row])

            self.log_message(f"CSV file saved: {file_path}")
            self.update_status(f"CSV downloaded: {table_name}", "#A0A0A0")
            self.current_csv_file = file_path
            return True
        except Exception as e:
            self.log_message(f"Error saving CSV: {str(e)}")
            self.update_status("Error saving CSV", "red")
            return False

    def view_csv(self):
        table_name = self.table_combo.currentText()
        self.log_message(f"Selected table: '{table_name}'")
        
        if not table_name:
            self.log_message("No table selected")
            self.update_status("No table selected", "red")
            return

        file_name = f"{table_name}.csv"
        file_path = os.path.join(os.getcwd(), file_name)

        if not os.path.exists(file_path):
            success = self.download_csv(table_name)
            if not success:
                return

        if os.path.exists(file_path):
            dialog = CsvViewerDialog(file_path)
            dialog.exec()
        else:
            self.log_message(f"CSV file not found: {file_name}")
            self.update_status("CSV file not found", "red")
        



    def upload_to_neo4j(self, label, file_path):
        try:
            with self.neo4j_driver.session() as session:
                # Clear existing nodes of this type
                session.run(f"MATCH (n:{label}) DETACH DELETE n")

                # Load CSV and create nodes
                cypher_query = f"""
                LOAD CSV WITH HEADERS FROM 'file:///{file_path}' AS row
                CREATE (:{label} {{
                    {', '.join([f"`{key}`: row.`{key}`" for key in pd.read_csv(file_path, nrows=1).columns])}
                }})
                """
                session.run(cypher_query)

            self.log_message(f"CSV data uploaded to Neo4j as {label} nodes")
        except Exception as e:
            self.log_message(f"Error uploading to Neo4j: {str(e)}")

    def upload_csv_to_neo4j(self):
        file_name, _ = QFileDialog.getOpenFileName(self, "Select CSV to upload to Neo4j", "", "CSV Files (*.csv)")
        if not file_name:
            return  # User cancelled the file selection

        try:
            # Get the label name from the file name (without extension)
            label = os.path.splitext(os.path.basename(file_name))[0]

            # Read the CSV file
            df = pd.read_csv(file_name)

            with self.neo4j_driver.session() as session:
                # Clear existing nodes of this type
                session.run(f"MATCH (n:{label}) DETACH DELETE n")

                # Create nodes from DataFrame
                for _, row in df.iterrows():
                    properties = ', '.join([f"`{col}`: ${col}" for col in df.columns])
                    cypher_query = f"""
                    CREATE (:{label} {{{properties}}})
                    """
                    session.run(cypher_query, **row.to_dict())

            self.log_message(f"CSV data uploaded to Neo4j as {label} nodes")
            self.update_status(f"Neo4j upload complete: {label}", "#A0A0A0")
        except Exception as e:
            self.log_message(f"Error uploading to Neo4j: {str(e)}")
            self.update_status("Neo4j upload failed", "red")               

    def log_message(self, message):
        self.log_text.append(message)

    def update_status(self, message, color):
        self.status_bar.showMessage(message, 5000)
        self.status_bar.setStyleSheet(f"background-color: {color}; color: white;")

    def closeEvent(self, event):
        if self.conn:
            self.conn.close()
        if hasattr(self, 'neo4j_driver'):
            self.neo4j_driver.close()
        self.log_message("Database connections closed")
        event.accept()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    viewer = DatabaseViewer()
    viewer.show()
    sys.exit(app.exec())