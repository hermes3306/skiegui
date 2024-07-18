import sys
import os
import pymongo
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QPushButton, QComboBox, QTextEdit, QLabel, QScrollArea, QLineEdit,
                             QMessageBox)
from PyQt5.QtGui import QColor, QTextCharFormat, QFont
from PyQt5.QtCore import Qt
import configparser
import json
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

class MongoDBGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.initUI()
        self.client = None
        self.db = None
        self.collection = None
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

        # Connection preview
        self.connection_preview = QTextEdit()
        self.connection_preview.setMaximumHeight(100)
        layout.addWidget(self.connection_preview)

        # Database section
        db_layout = QHBoxLayout()
        self.db_combo = EditableComboBox()
        self.create_db_button = QPushButton('Create DB')
        self.create_db_button.clicked.connect(self.create_database)
        db_layout.addWidget(self.db_combo)
        db_layout.addWidget(self.create_db_button)
        layout.addLayout(db_layout)

        # Collection section
        collection_layout = QHBoxLayout()
        self.collection_combo = EditableComboBox()
        self.create_collection_button = QPushButton('Create Collection')
        self.create_collection_button.clicked.connect(self.create_collection)
        collection_layout.addWidget(self.collection_combo)
        collection_layout.addWidget(self.create_collection_button)
        layout.addLayout(collection_layout)

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
        self.query_button = QPushButton('Query')
        self.query_button.clicked.connect(self.query_data)
        query_layout.addWidget(self.query_combo)
        query_layout.addWidget(self.query_button)
        layout.addLayout(query_layout)

        # Query preview
        self.query_preview = QTextEdit()
        self.query_preview.setMaximumHeight(100)
        layout.addWidget(self.query_preview)

        # Result panel
        self.result_text = QTextEdit()
        self.result_text.setReadOnly(True)
        scroll_area = QScrollArea()
        scroll_area.setWidget(self.result_text)
        scroll_area.setWidgetResizable(True)
        layout.addWidget(scroll_area)

        # Connect combo box changes to preview updates
        self.connection_combo.currentTextChanged.connect(self.update_connection_preview)
        self.insert_combo.currentTextChanged.connect(self.update_insert_preview)
        self.query_combo.currentTextChanged.connect(self.update_query_preview)

    def load_config(self):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        config = configparser.ConfigParser()
        config.read(os.path.join(script_dir, 'mongo.ini'))
        for section in config.sections():
            self.connection_combo.addItem(section)

        config.read(os.path.join(script_dir, 'ins.ini'))
        for section in config.sections():
            self.insert_combo.addItem(section)

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
            self.connection_preview.setPlainText(connection_string)
        else:
            self.connection_preview.setPlainText("")

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
        connection_string = self.connection_preview.toPlainText()
        
        # Save the potentially updated connection string
        new_section = self.save_config('mongo.ini', connection_name, 'connection_string', connection_string)
        if new_section != connection_name:
            self.connection_combo.setCurrentText(new_section)
        
        try:
            self.client = pymongo.MongoClient(connection_string)
            self.client.server_info()  # This will raise an exception if the connection fails
            self.update_db_list()
            self.log_message(f"Connected to MongoDB: {new_section}", "green")
        except Exception as e:
            self.log_message(f"Connection failed: {str(e)}", "red")

    def insert_update_data(self):
        if not self.db or not self.collection_combo.currentText():
            self.log_message("Please select a database and collection", "red")
            return

        insert_name = self.insert_combo.currentText()
        insert_code = self.insert_preview.toPlainText()
        
        # Save the potentially updated insert code
        new_section = self.save_config('ins.ini', insert_name, 'code', insert_code)
        if new_section != insert_name:
            self.insert_combo.setCurrentText(new_section)

        try:
            data = json.loads(insert_code)
            self.collection = self.db[self.collection_combo.currentText()]
            if '_id' in data:
                self.collection.update_one({'_id': data['_id']}, {'$set': data}, upsert=True)
            else:
                self.collection.insert_one(data)
            self.log_message("Data inserted/updated successfully", "green")
        except Exception as e:
            self.log_message(f"Error inserting/updating data: {str(e)}", "red")

    def query_data(self):
        if not self.db or not self.collection_combo.currentText():
            self.log_message("Please select a database and collection", "red")
            return

        query_name = self.query_combo.currentText()
        query_code = self.query_preview.toPlainText()
        
        # Save the potentially updated query code
        new_section = self.save_config('qry.ini', query_name, 'code', query_code)
        if new_section != query_name:
            self.query_combo.setCurrentText(new_section)

        try:
            query = json.loads(query_code)
            self.collection = self.db[self.collection_combo.currentText()]
            results = self.collection.find(query)
            self.display_results(results)
        except Exception as e:
            self.log_message(f"Error querying data: {str(e)}", "red")

    def update_db_list(self):
        self.db_combo.clear()
        for db_name in self.client.list_database_names():
            self.db_combo.addItem(db_name)

    def create_database(self):
        db_name = self.db_combo.currentText()
        if not db_name:
            self.log_message("Please enter a database name", "red")
            return
        self.db = self.client[db_name]
        self.db.create_collection('temp')  # MongoDB creates databases lazily
        self.update_db_list()
        self.log_message(f"Database '{db_name}' created", "green")

    def update_collection_list(self):
        self.collection_combo.clear()
        if self.db:
            for collection_name in self.db.list_collection_names():
                self.collection_combo.addItem(collection_name)

    def create_collection(self):
        collection_name = self.collection_combo.currentText()
        if not collection_name:
            self.log_message("Please enter a collection name", "red")
            return
        if self.db:
            self.db.create_collection(collection_name)
            self.update_collection_list()
            self.log_message(f"Collection '{collection_name}' created", "green")
        else:
            self.log_message("Please select a database first", "red")

    def display_results(self, results):
        self.result_text.clear()
        for doc in results:
            self.result_text.appendPlainText(json.dumps(doc, indent=2))
            self.result_text.appendPlainText("\n")

    def log_message(self, message, color):
        format = QTextCharFormat()
        format.setForeground(QColor(color))
        self.result_text.mergeCurrentCharFormat(format)
        self.result_text.appendPlainText(message)
        format.setForeground(QColor("black"))
        self.result_text.mergeCurrentCharFormat(format)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = MongoDBGUI()
    ex.show()
    sys.exit(app.exec_())