import sys
import os
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QPushButton, QComboBox, QTextEdit, QLabel, QScrollArea, QLineEdit,
                             QMessageBox, QTableWidget, QTableWidgetItem, QSizePolicy,QSplitter)
from PyQt5.QtWidgets import QSizePolicy, QPlainTextEdit
import pymongo
import traceback

from PyQt5.QtGui import QColor, QTextCharFormat, QFont
from PyQt5.QtCore import Qt
import configparser
import json
from bson import ObjectId
from bson.json_util import dumps
from datetime import datetime


class EditableComboBox(QComboBox):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setEditable(True)
        self.lineEdit().returnPressed.connect(self.add_current_text)


    def add_current_text(self):
        current_text = self.currentText()
        if self.findText(current_text) == -1:
            self.addItem(current_text)

class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)

class MongoDBGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.initUI()
        self.client = None
        self.db = None
        self.collection = None
        self.statusBar = self.statusBar()
        self.load_config()

    def initUI(self):
        self.setWindowTitle('MongoDB GUI')
        self.setGeometry(100, 100, 1000, 800)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        # Connection section
        connection_layout = QHBoxLayout()
        self.connection_combo = EditableComboBox()
        self.connect_button = QPushButton('Connect')
        self.connect_button.clicked.connect(self.connect_to_mongodb)
        connection_layout.addWidget(self.connection_combo)
        connection_layout.addWidget(self.connect_button)
        layout.addLayout(connection_layout)

        
        # Database section
        db_layout = QHBoxLayout()
        self.db_combo = EditableComboBox()
        self.create_db_button = QPushButton('Create or Use DB')
        self.create_db_button.clicked.connect(self.create_or_use_database)
        self.delete_db_button = QPushButton('Delete DB')
        self.delete_db_button.clicked.connect(self.delete_database)
        db_layout.addWidget(self.db_combo)
        db_layout.addWidget(self.create_db_button)
        db_layout.addWidget(self.delete_db_button)
        layout.addLayout(db_layout)

        # Collection section
        collection_layout = QHBoxLayout()
        self.collection_combo = EditableComboBox()
        self.create_collection_button = QPushButton('Create or Use Collection')
        self.create_collection_button.clicked.connect(self.create_or_use_collection)
        self.delete_collection_button = QPushButton('Delete Collection')
        self.delete_collection_button.clicked.connect(self.delete_collection)
        collection_layout.addWidget(self.collection_combo)
        collection_layout.addWidget(self.create_collection_button)
        collection_layout.addWidget(self.delete_collection_button)
        layout.addLayout(collection_layout)
        
        # Change the connection preview to a single line
        self.connection_preview = QLineEdit()
        self.connection_preview.setStyleSheet("""
            QLineEdit {
                color: #00FF00;  /* Bright green text */
                background-color: #000000;  /* Black background */
                border: 1px solid #00FF00;  /* Green border */
                padding: 2px;
            }
        """)
        layout.addWidget(self.connection_preview)

        # Insert/Update section
        insert_layout = QHBoxLayout()
        self.insert_combo = EditableComboBox()
        self.insert_button = QPushButton('Insert/Update')
        self.insert_button.clicked.connect(self.insert_update_data)
        insert_layout.addWidget(self.insert_combo)
        insert_layout.addWidget(self.insert_button)
        layout.addLayout(insert_layout)

        # Insert preview
        self.insert_preview = QTextEdit()
        self.insert_preview.setMaximumHeight(100)
        layout.addWidget(self.insert_preview)

        # Query section
        query_layout = QHBoxLayout()
        self.query_combo = EditableComboBox()
        self.result_form_combo = QComboBox()
        self.result_form_combo.addItems(["Table", "Plain Text"])
        self.query_button = QPushButton('Query')
        self.query_button.clicked.connect(self.query_data)
        query_layout.addWidget(self.query_combo)
        query_layout.addWidget(self.result_form_combo)
        query_layout.addWidget(self.query_button)
        layout.addLayout(query_layout)

        # Query preview
        self.query_preview = QTextEdit()
        self.query_preview.setMaximumHeight(100)
        layout.addWidget(self.query_preview)

        # Result area for table
        self.result_area = QScrollArea()
        self.result_area.setWidgetResizable(True)
        layout.addWidget(self.result_area)

        # Log area
        self.result_text = QTextEdit()
        self.result_text.setReadOnly(True)
        scroll_area = QScrollArea()
        scroll_area.setWidget(self.result_text)
        scroll_area.setWidgetResizable(True)
        scroll_area.setMaximumHeight(150)  # Set a maximum height of 100 pixels
        scroll_area.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        layout.addWidget(scroll_area)

        # scroll_area = QScrollArea()
        # scroll_area.setWidget(self.result_text)
        # scroll_area.setWidgetResizable(True)
        # layout.addWidget(scroll_area)

        # Connect combo box changes to preview updates
        self.connection_combo.currentTextChanged.connect(self.update_connection_preview)
        self.insert_combo.currentTextChanged.connect(self.update_insert_preview)
        self.query_combo.currentTextChanged.connect(self.update_query_preview)
        self.display_table_results([])

    def load_config(self):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Load connection configurations
        config = configparser.ConfigParser()
        config.read(os.path.join(script_dir, 'mongo.ini'))
        for section in config.sections():
            self.connection_combo.addItem(section)

        # Load insert configurations
        config.clear()
        config.read(os.path.join(script_dir, 'ins.ini'))
        for section in config.sections():
            self.insert_combo.addItem(section)

        # Load query configurations
        config.clear()
        config.read(os.path.join(script_dir, 'qry.ini'))
        for section in config.sections():
            self.query_combo.addItem(section)

        # Update previews for initial selections
        self.update_connection_preview()
        self.update_insert_preview()
        self.update_query_preview()

    def update_connection_preview(self):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config = configparser.ConfigParser()
        config.read(os.path.join(script_dir, 'mongo.ini'))
        connection_name = self.connection_combo.currentText()
        if connection_name in config:
            connection_string = config[connection_name]['connection_string']
            self.connection_preview.setText(connection_string)  # Already correct in the previous update
        else:
            self.connection_preview.setText("")  # Already correct in the previous update

    def update_insert_preview(self):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config = configparser.ConfigParser()
        config.read(os.path.join(script_dir, 'ins.ini'))
        insert_name = self.insert_combo.currentText()
        if insert_name in config:
            insert_code = config[insert_name]['code']
            self.insert_preview.setPlainText(insert_code)
        else:
            self.insert_preview.setPlainText("")

    def update_query_preview(self):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config = configparser.ConfigParser()
        config.read(os.path.join(script_dir, 'qry.ini'))
        query_name = self.query_combo.currentText()
        if query_name in config:
            query_code = config[query_name]['code']
            self.query_preview.setPlainText(query_code)
        else:
            self.query_preview.setPlainText("")    

    def save_config(self, file_name, section, key, value):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config = configparser.ConfigParser()
        config.read(os.path.join(script_dir, file_name))
        
        if section not in config:
            config[section] = {}
        
        if config[section].get(key, "") != value:
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            new_section = f"{section}_{timestamp}"
            config[new_section] = {key: value}
            with open(os.path.join(script_dir, file_name), 'w') as configfile:
                config.write(configfile)
            return new_section
        return section
            
    def connect_to_mongodb(self):
        connection_name = self.connection_combo.currentText()
        connection_string = self.connection_preview.text()
        
        self.log_message(f"Attempting to connect to MongoDB: {connection_name}")
        
        new_section = self.save_config('mongo.ini', connection_name, 'connection_string', connection_string)
        if new_section != connection_name:
            self.connection_combo.setCurrentText(new_section)
            self.log_message(f"Created new connection config: {new_section}")
        
        try:
            self.client = pymongo.MongoClient(connection_string)
            self.client.server_info()  # This will raise an exception if the connection fails
            self.update_db_list()
            self.log_message(f"Successfully connected to MongoDB: {new_section}", "success")
        except Exception as e:
            self.log_message(f"Connection failed: {str(e)}", "error")

    def create_or_use_database(self):
        db_name = self.db_combo.currentText()
        if not db_name:
            self.log_message("Please enter a database name", "error")
            return
        
        if self.client is None:
            self.log_message("Please connect to MongoDB first", "error")
            return

        self.log_message(f"Attempting to create or use database: {db_name}")

        try:
            self.db = self.client[db_name]
            if db_name in self.client.list_database_names():
                self.log_message(f"Using existing database: '{db_name}'")
            else:
                self.db.create_collection('temp')
                self.db.drop_collection('temp')
                self.log_message(f"Database '{db_name}' created", "success")
            
            self.update_db_list()
            self.db_combo.setCurrentText(db_name)
            self.update_collection_list()
        except Exception as e:
            self.log_message(f"Error creating/using database: {str(e)}", "error")
            self.db = None

    def create_or_use_collection(self):
        if self.client is None:
            self.log_message("Please connect to MongoDB first", "error")
            return

        db_name = self.db_combo.currentText()
        if not db_name:
            self.log_message("Please select a database", "error")
            return

        collection_name = self.collection_combo.currentText()
        if not collection_name:
            self.log_message("Please enter a collection name", "error")
            return

        self.log_message(f"Attempting to create or use collection: {collection_name} in database: {db_name}")

        try:
            self.db = self.client[db_name]
            
            if collection_name in self.db.list_collection_names():
                self.log_message(f"Using existing collection: '{collection_name}'")
            else:
                self.db.create_collection(collection_name)
                self.log_message(f"Collection '{collection_name}' created", "success")
            
            self.collection = self.db[collection_name]
            self.update_collection_list()
            self.collection_combo.setCurrentText(collection_name)
        except Exception as e:
            self.log_message(f"Error creating/using collection: {str(e)}", "error")
            self.collection = None
    
    def update_db_list(self):
        self.db_combo.clear()
        for db_name in self.client.list_database_names():
            self.db_combo.addItem(db_name)
            
    def update_collection_list(self):
        self.collection_combo.clear()
        if self.db is not None:
           for collection_name in self.db.list_collection_names():
                self.collection_combo.addItem(collection_name)

    def display_results(self, results):
        self.result_text.clear()
        for doc in results:
            self.result_text.appendPlainText(json.dumps(doc, indent=2))
            self.result_text.appendPlainText("\n")

    def insert_update_data(self):
        connection_name = self.connection_combo.currentText()
        db_name = self.db_combo.currentText()
        collection_name = self.collection_combo.currentText()
        
        if not connection_name or not db_name or not collection_name:
            self.log_message("Please select a connection, database, and collection", "error")
            return

        self.log_message(f"Attempting to insert/update data in {db_name}.{collection_name}")

        script_dir = os.path.dirname(os.path.abspath(__file__))
        config = configparser.ConfigParser()
        config.read(os.path.join(script_dir, 'mongo.ini'))
        if connection_name not in config:
            self.log_message(f"Connection '{connection_name}' not found in config", "error")
            return
        connection_string = config[connection_name]['connection_string']

        try:
            client = pymongo.MongoClient(connection_string)
            db = client[db_name]
            collection = db[collection_name]
        except Exception as e:
            self.log_message(f"Error connecting to MongoDB: {str(e)}", "error")
            return

        insert_name = self.insert_combo.currentText()
        insert_code = self.insert_preview.toPlainText()

        try:
            data = json.loads(insert_code)
            if '_id' in data:
                result = collection.update_one({'_id': data['_id']}, {'$set': data}, upsert=True)
                if result.modified_count > 0:
                    self.log_message("Document updated successfully", "success")
                elif result.upserted_id:
                    self.log_message("Document inserted successfully", "success")
                else:
                    self.log_message("No changes made to the document")
            else:
                result = collection.insert_one(data)
                self.log_message(f"Document inserted successfully with id: {result.inserted_id}", "success")
        except json.JSONDecodeError:
            self.log_message("Invalid JSON in insert/update data", "error")
        except Exception as e:
            self.log_message(f"Error inserting/updating data: {str(e)}", "error")
        finally:
            client.close()

    def delete_database(self):
        db_name = self.db_combo.currentText()
        if not db_name:
            self.log_message("Please select a database to delete", "error")
            return
        
        if self.client is None:
            self.log_message("Please connect to MongoDB first", "error")
            return

        reply = QMessageBox.question(self, 'Delete Database',
                                    f"Are you sure you want to delete the database '{db_name}'?",
                                    QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        
        if reply == QMessageBox.Yes:
            try:
                self.client.drop_database(db_name)
                self.log_message(f"Database '{db_name}' deleted successfully", "success")
                self.update_db_list()
                self.db = None
                self.collection = None
                self.update_collection_list()
            except Exception as e:
                self.log_message(f"Error deleting database: {str(e)}", "error")

    def delete_collection(self):
        db_name = self.db_combo.currentText()
        collection_name = self.collection_combo.currentText()
        
        if not db_name or not collection_name:
            self.log_message("Please select a database and collection to delete", "error")
            return
        
        if self.client is None or self.db is None:
            self.log_message("Please connect to MongoDB and select a database first", "error")
            return

        reply = QMessageBox.question(self, 'Delete Collection',
                                    f"Are you sure you want to delete the collection '{collection_name}' from database '{db_name}'?",
                                    QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        
        if reply == QMessageBox.Yes:
            try:
                self.db.drop_collection(collection_name)
                self.log_message(f"Collection '{collection_name}' deleted successfully", "success")
                self.update_collection_list()
                self.collection = None
            except Exception as e:
                self.log_message(f"Error deleting collection: {str(e)}", "error")
                
    def query_data(self):
        connection_name = self.connection_combo.currentText()
        db_name = self.db_combo.currentText()
        collection_name = self.collection_combo.currentText()
        
        if not connection_name or not db_name or not collection_name:
            self.log_message("Please select a connection, database, and collection", "error")
            return

        self.log_message(f"Attempting to query data from {db_name}.{collection_name}")

        script_dir = os.path.dirname(os.path.abspath(__file__))
        config = configparser.ConfigParser()
        config.read(os.path.join(script_dir, 'mongo.ini'))
        if connection_name not in config:
            self.log_message(f"Connection '{connection_name}' not found in config", "error")
            return
        connection_string = config[connection_name]['connection_string']

        try:
            client = pymongo.MongoClient(connection_string)
            db = client[db_name]
            collection = db[collection_name]
        except Exception as e:
            self.log_message(f"Error connecting to MongoDB: {str(e)}", "error")
            return

        query_name = self.query_combo.currentText()
        query_code = self.query_preview.toPlainText()
    

        try:
            query = json.loads(query_code)
            results = list(collection.find(query))
            
            if self.result_form_combo.currentText() == "Plain Text":
                self.result_text.clear()
                for doc in results:
                    json_string = json.dumps(doc, indent=2, cls=MongoJSONEncoder)
                    self.log_message(json_string)
            else:  # Table format
                self.display_table_results(results)
            
            if results:
                self.log_message(f"Query executed successfully. Found {len(results)} document(s).", "success")
            else:
                self.log_message("Query executed successfully. No documents found.")
                if self.result_form_combo.currentText() == "Table":
                    self.display_table_results([])  # Display empty table

        except json.JSONDecodeError:
            self.log_message("Invalid JSON in query data", "error")
            self.display_table_results([])  # Display empty table
        except Exception as e:
            error_message = f"Error executing query: {str(e)}\n\nStack trace:\n{traceback.format_exc()}"
            self.log_message(error_message, "error")
            self.display_table_results([])  # Display empty table
        finally:
            client.close()
        

    def display_table_results_old(self, documents):
        if not documents:
            return

        # Create table
        table = QTableWidget()

        # Set row and column count
        table.setRowCount(len(documents))
        headers = set()
        for doc in documents:
            headers.update(doc.keys())
        table.setColumnCount(len(headers))

        # Set headers
        table.setHorizontalHeaderLabels(list(headers))

        # Populate table
        for row, doc in enumerate(documents):
            for col, header in enumerate(headers):
                item = QTableWidgetItem(str(doc.get(header, "")))
                table.setItem(row, col, item)

        # Resize columns to content
        table.resizeColumnsToContents()

        # Set the table as the widget for the scroll area
        self.result_area.setWidget(table)

        # Ensure the table takes up all available space
        table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

    def display_table_results(self, documents):
        # Create table
        table = QTableWidget()

        if documents:
            # Set row and column count
            table.setRowCount(len(documents))
            headers = set()
            for doc in documents:
                headers.update(doc.keys())
            table.setColumnCount(len(headers))

            # Set headers
            table.setHorizontalHeaderLabels(list(headers))

            # Populate table
            for row, doc in enumerate(documents):
                for col, header in enumerate(headers):
                    item = QTableWidgetItem(str(doc.get(header, "")))
                    table.setItem(row, col, item)

            # Resize columns to content
            table.resizeColumnsToContents()
        else:
            # Create an empty table with default headers
            default_headers = ["_id", "Field1", "Field2", "Field3"]
            table.setRowCount(0)
            table.setColumnCount(len(default_headers))
            table.setHorizontalHeaderLabels(default_headers)

        # Set the table as the widget for the scroll area
        self.result_area.setWidget(table)

        # Ensure the table takes up all available space
        table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

    def log_message_old(self, message, message_type="info"):
        cursor = self.result_text.textCursor()
        format = QTextCharFormat()
        
        if message_type == "error":
            format.setForeground(QColor("red"))
        elif message_type == "success":
            format.setForeground(QColor("green"))
        else:  # info
            format.setForeground(QColor("blue"))
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_message = f"[{timestamp}] {message}\n"
        
        cursor.insertText(formatted_message, format)
        self.result_text.setTextCursor(cursor)
        self.result_text.ensureCursorVisible()

    def log_message(self, message, message_type="info"):
        cursor = self.result_text.textCursor()
        format = QTextCharFormat()
        
        if message_type == "error":
            format.setForeground(QColor("red"))
        elif message_type == "success":
            format.setForeground(QColor("green"))
        else:  # info
            format.setForeground(QColor("blue"))
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_message = f"[{timestamp}] {message}\n"
        
        cursor.insertText(formatted_message, format)
        self.result_text.setTextCursor(cursor)
        self.result_text.ensureCursorVisible()
        
        # Scroll to the bottom to show the latest message
        self.result_text.verticalScrollBar().setValue(self.result_text.verticalScrollBar().maximum())
        
    def log_message_statusbar(self, message, message_type="info"):
        if message_type == "error":
            self.statusBar.setStyleSheet("color: red")
        elif message_type == "success":
            self.statusBar.setStyleSheet("color: green")
        else:  # info
            self.statusBar.setStyleSheet("color: blue")
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_message = f"[{timestamp}] {message}"
        self.statusBar.showMessage(formatted_message, 5000)
        
if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = MongoDBGUI()
    ex.show()
    sys.exit(app.exec_())