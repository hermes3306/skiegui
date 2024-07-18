import sys
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QPushButton, QLineEdit, QLabel, QTextEdit, QMessageBox, QInputDialog,
                             QTableWidget, QTableWidgetItem)
from PyQt6.QtCore import Qt
from pymongo import MongoClient
from pymongo.errors import PyMongoError

class MongoDBManager(QMainWindow):
    def __init__(self):
        super().__init__()
        self.client = None
        self.db = None
        self.collection = None  # Initialize collection attribute
        self.default_uri = "mongodb+srv://bstyfs23:eZVn53ELtH7jGF4G@cluster0.uo0pbrc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        self.initUI()

    def initUI(self):
        self.setWindowTitle('MongoDB Manager')
        self.setGeometry(100, 100, 800, 600)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        main_layout = QVBoxLayout()
        central_widget.setLayout(main_layout)

        # Connection section
        connection_layout = QHBoxLayout()
        self.uri_input = QLineEdit(self.default_uri)  # Set default URI
        self.uri_input.setPlaceholderText("MongoDB URI")
        connection_layout.addWidget(self.uri_input)
        
        connect_button = QPushButton("Connect")
        connect_button.clicked.connect(self.connect_to_mongodb)
        connection_layout.addWidget(connect_button)

        main_layout.addLayout(connection_layout)

        # Database and Collection section
        db_collection_layout = QHBoxLayout()
        self.db_input = QLineEdit()
        self.db_input.setPlaceholderText("Database Name")
        db_collection_layout.addWidget(self.db_input)

        self.collection_input = QLineEdit()
        self.collection_input.setPlaceholderText("Collection Name")
        db_collection_layout.addWidget(self.collection_input)

        use_db_button = QPushButton("Use DB/Collection")
        use_db_button.clicked.connect(self.use_db_and_collection)
        db_collection_layout.addWidget(use_db_button)

        main_layout.addLayout(db_collection_layout)

        # Insert Document section
        insert_layout = QHBoxLayout()
        self.insert_input = QTextEdit()
        self.insert_input.setPlaceholderText("Enter document in JSON format")
        insert_layout.addWidget(self.insert_input)

        insert_button = QPushButton("Insert Document")
        insert_button.clicked.connect(self.insert_document)
        insert_layout.addWidget(insert_button)

        main_layout.addLayout(insert_layout)

        # Query section
        query_layout = QHBoxLayout()
        self.query_input = QLineEdit()
        self.query_input.setPlaceholderText("Enter query in JSON format")
        query_layout.addWidget(self.query_input)

        query_button = QPushButton("Run Query")
        query_button.clicked.connect(self.run_query)
        query_layout.addWidget(query_button)

        main_layout.addLayout(query_layout)

        # Results section
        self.results_table = QTableWidget()
        main_layout.addWidget(self.results_table)

        # Additional buttons
        button_layout = QHBoxLayout()
        
        create_collection_button = QPushButton("Create Collection")
        create_collection_button.clicked.connect(self.create_collection)
        button_layout.addWidget(create_collection_button)

        drop_collection_button = QPushButton("Drop Collection")
        drop_collection_button.clicked.connect(self.drop_collection)
        button_layout.addWidget(drop_collection_button)

        list_collections_button = QPushButton("List Collections")
        list_collections_button.clicked.connect(self.list_collections)
        button_layout.addWidget(list_collections_button)

        main_layout.addLayout(button_layout)

    def connect_to_mongodb(self):
        uri = self.uri_input.text()
        try:
            self.client = MongoClient(uri)
            self.client.server_info()  # Will raise an exception if connection fails
            QMessageBox.information(self, "Success", "Connected to MongoDB successfully!")
        except PyMongoError as e:
            QMessageBox.critical(self, "Error", f"Failed to connect to MongoDB: {str(e)}")


    def use_db_and_collection(self):
        if not self.client:
            QMessageBox.warning(self, "Warning", "Please connect to MongoDB first.")
            return

        db_name = self.db_input.text()
        collection_name = self.collection_input.text()

        if not db_name or not collection_name:
            QMessageBox.warning(self, "Warning", "Please enter both database and collection names.")
            return

        try:
            self.db = self.client[db_name]
            self.collection = self.db[collection_name]
            QMessageBox.information(self, "Success", f"Using database '{db_name}' and collection '{collection_name}'")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to use database or collection: {str(e)}")
            self.db = None
            self.collection = None

    def insert_document(self):
        if not self.client:
            QMessageBox.warning(self, "Warning", "Please connect to MongoDB first.")
            return

        if not self.db or not self.collection:
            QMessageBox.warning(self, "Warning", "Please select a database and collection first.")
            return

        try:
            doc = eval(self.insert_input.toPlainText())
            result = self.collection.insert_one(doc)
            QMessageBox.information(self, "Success", f"Document inserted with ID: {result.inserted_id}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to insert document: {str(e)}")



    def run_query(self):
        if not self.collection:
            QMessageBox.warning(self, "Warning", "Please select a collection first.")
            return

        try:
            query = eval(self.query_input.text())
            results = list(self.collection.find(query))
            self.display_results(results)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to run query: {str(e)}")

    def display_results(self, results):
        self.results_table.clear()
        if not results:
            QMessageBox.information(self, "Info", "No results found.")
            return

        # Set up the table
        self.results_table.setColumnCount(len(results[0]))
        self.results_table.setRowCount(len(results))
        self.results_table.setHorizontalHeaderLabels(results[0].keys())

        # Populate the table
        for row, doc in enumerate(results):
            for col, (key, value) in enumerate(doc.items()):
                self.results_table.setItem(row, col, QTableWidgetItem(str(value)))

        self.results_table.resizeColumnsToContents()

    def create_collection(self):
        if not self.db:
            QMessageBox.warning(self, "Warning", "Please select a database first.")
            return

        name, ok = QInputDialog.getText(self, "Create Collection", "Enter collection name:")
        if ok and name:
            try:
                self.db.create_collection(name)
                QMessageBox.information(self, "Success", f"Collection '{name}' created successfully.")
            except PyMongoError as e:
                QMessageBox.critical(self, "Error", f"Failed to create collection: {str(e)}")

    def drop_collection(self):
        if not self.db:
            QMessageBox.warning(self, "Warning", "Please select a database first.")
            return

        name, ok = QInputDialog.getText(self, "Drop Collection", "Enter collection name to drop:")
        if ok and name:
            try:
                self.db.drop_collection(name)
                QMessageBox.information(self, "Success", f"Collection '{name}' dropped successfully.")
            except PyMongoError as e:
                QMessageBox.critical(self, "Error", f"Failed to drop collection: {str(e)}")

    def list_collections(self):
        if not self.db:
            QMessageBox.warning(self, "Warning", "Please select a database first.")
            return

        try:
            collections = self.db.list_collection_names()
            QMessageBox.information(self, "Collections", f"Collections in the database:\n{', '.join(collections)}")
        except PyMongoError as e:
            QMessageBox.critical(self, "Error", f"Failed to list collections: {str(e)}")

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = MongoDBManager()
    ex.show()
    sys.exit(app.exec())