import sys
import configparser
import psycopg2
from neo4j import GraphDatabase
from PyQt5.QtWidgets import (QApplication, QWidget, QLabel, QLineEdit, QPushButton, 
                             QVBoxLayout, QHBoxLayout, QTableWidget, QTableWidgetItem, 
                             QGroupBox, QTextEdit, QMessageBox, QComboBox, QProgressBar,
                             QMainWindow, QAction, QMenu, QDialog, QGridLayout, QInputDialog,
                             QScrollArea, QDialogButtonBox)
from PyQt5.QtCore import Qt, QSize
import sqlparse
import re

class QueryEditDialog(QDialog):
    def __init__(self, parent=None, query_text=""):
        super().__init__(parent)
        self.setWindowTitle("Edit Query")
        self.resize(600, 400)  # Set a reasonable size for the dialog

        layout = QVBoxLayout(self)

        self.query_edit = QTextEdit(self)
        self.query_edit.setPlainText(query_text)
        layout.addWidget(self.query_edit)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def get_edited_query(self):
        return self.query_edit.toPlainText()


class DatabaseMigrationGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.pg_connections = self.load_pg_connections('pg.ini')
        self.neo4j_connections = self.load_neo4j_connections('neo4j.ini')
        self.saved_sql_queries = self.load_queries('sql.ini')
        self.saved_cypher_queries = self.load_queries('cypher.ini')
        self.load_style('style_light.ini')  # Load the style
        self.initUI()

    def load_style(self, filename):
        config = configparser.ConfigParser()
        config.read(filename)
        style = config['Style']['stylesheet']
        self.setStyleSheet(style)

    def load_queries(self, filename):
        config = configparser.ConfigParser()
        config.read(filename)
        return {section: config[section]['query'] for section in config.sections()}

    def save_queries(self, filename, queries):
        config = configparser.ConfigParser()
        for name, query in queries.items():
            config[name] = {'query': query}
        with open(filename, 'w') as configfile:
            config.write(configfile)

    def load_pg_connections(self, filename):
        config = configparser.ConfigParser()
        config.read(filename)
        return {section: dict(config[section]) for section in config.sections()}

    def load_neo4j_connections(self, filename):
        config = configparser.ConfigParser()
        config.read(filename)
        return {section: dict(config[section]) for section in config.sections()}

    def initUI(self):
        self.setWindowTitle('PostgreSQL to Neo4j Migration Tool')

        # Create menu bar
        self.create_menu_bar()

        # Create central widget and layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # Rest of the UI components (same as before)
        db_layout = QHBoxLayout()
        db_layout.addWidget(self.create_postgresql_group())
        db_layout.addWidget(self.create_neo4j_group())
        main_layout.addLayout(db_layout)
        
        query_layout = QHBoxLayout()
        query_layout.addWidget(self.create_sql_query_group())
        query_layout.addWidget(self.create_cypher_query_group())
        main_layout.addLayout(query_layout)

        migration_layout = QHBoxLayout()
        self.table_dropdown = QComboBox()
        self.table_dropdown.addItem("Select a table")
        migration_layout.addWidget(self.table_dropdown)

        self.migrate_selected_button = QPushButton("Migrate selected table")
        self.migrate_selected_button.clicked.connect(self.migrate_selected_table)
        migration_layout.addWidget(self.migrate_selected_button)

        self.migrate_all_button = QPushButton("Migrate all tables")
        self.migrate_all_button.clicked.connect(self.migrate_all_tables)
        migration_layout.addWidget(self.migrate_all_button)

        main_layout.addLayout(migration_layout)

        self.progress_bar = QProgressBar(self)
        main_layout.addWidget(self.progress_bar)

        welcome_text = ("Welcome to PostgreSQL to Neo4j migration tool.\n"
                        "Test connection to each DBMS and select a table to migrate or migrate all tables:")
        welcome_label = QLabel(welcome_text)
        main_layout.addWidget(welcome_label)

        self.status_box = QTextEdit()
        self.status_box.setReadOnly(True)
        main_layout.addWidget(self.status_box)

        self.show()

    def create_menu_bar(self):
        menubar = self.menuBar()

        # File menu
        file_menu = menubar.addMenu('File')
        
        # Open submenu
        open_menu = QMenu('Open', self)
        file_menu.addMenu(open_menu)
        
        pg_ini_action = QAction('pg.ini', self)
        pg_ini_action.triggered.connect(self.edit_pg_ini)
        open_menu.addAction(pg_ini_action)
        
        neo4j_ini_action = QAction('neo4j.ini', self)
        neo4j_ini_action.triggered.connect(self.edit_neo4j_ini)
        open_menu.addAction(neo4j_ini_action)

        exit_action = QAction('Exit', self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # Other top-level menus (without subitems for now)
        menubar.addMenu('Edit')
        menubar.addMenu('Selection')
        menubar.addMenu('View')
        menubar.addMenu('Go')
        menubar.addMenu('Run')
        menubar.addMenu('Terminal')
        menubar.addMenu('Help')

    def edit_pg_ini(self):
        self.edit_ini_file('pg.ini')

    def edit_neo4j_ini(self):
        self.edit_ini_file('neo4j.ini')

    def edit_ini_file(self, filename):
        config = configparser.ConfigParser()
        config.read(filename)

        dialog = QDialog(self)
        dialog.setWindowTitle(f"Edit {filename}")
        
        # Set the size of the dialog
        dialog.resize(700,700)  # Width: 600px, Height: 400px

        # Create a scroll area
        scroll = QScrollArea(dialog)
        scroll.setWidgetResizable(True)

        # Create a widget to hold the layout
        content_widget = QWidget()
        scroll.setWidget(content_widget)

        layout = QGridLayout(content_widget)

        widgets = {}
        self.row = 0

        for section in config.sections():
            layout.addWidget(QLabel(f"[{section}]"), self.row, 0, 1, 2)
            self.row += 1
            for key, value in config[section].items():
                layout.addWidget(QLabel(key), self.row, 0)
                line_edit = QLineEdit(value)
                layout.addWidget(line_edit, self.row, 1)
                widgets[(section, key)] = line_edit
                self.row += 1

        add_button = QPushButton("Add New Connection")
        add_button.clicked.connect(lambda: self.add_new_connection(config, widgets, layout))
        layout.addWidget(add_button, self.row, 0, 1, 2)
        self.row += 1

        self.save_button = QPushButton("Save")
        self.save_button.clicked.connect(lambda: self.save_ini_file(filename, config, widgets, dialog))
        layout.addWidget(self.save_button, self.row, 0, 1, 2)

        # Create a layout for the dialog and add the scroll area
        dialog_layout = QVBoxLayout(dialog)
        dialog_layout.addWidget(scroll)

        dialog.exec_()

    def add_new_connection(self, config, widgets, layout):
        connection_name, ok = QInputDialog.getText(self, "New Connection", "Enter connection name:")
        if ok and connection_name:
            if connection_name in config.sections():
                QMessageBox.warning(self, "Warning", "Connection name already exists.")
                return

            config[connection_name] = {}
            layout.addWidget(QLabel(f"[{connection_name}]"), self.row, 0, 1, 2)
            self.row += 1
            fields = ['url', 'user', 'password']
            for field in fields:
                layout.addWidget(QLabel(field), self.row, 0)
                line_edit = QLineEdit()
                layout.addWidget(line_edit, self.row, 1)
                widgets[(connection_name, field)] = line_edit
                self.row += 1

            # Move the "Add New Connection" and "Save" buttons to the bottom
            add_button = layout.itemAtPosition(self.row - 1, 0).widget()
            layout.removeWidget(add_button)
            layout.addWidget(add_button, self.row, 0, 1, 2)
            self.row += 1

            layout.removeWidget(self.save_button)
            layout.addWidget(self.save_button, self.row, 0, 1, 2)

    def save_ini_file(self, filename, config, widgets, dialog):
        for (section, key), widget in widgets.items():
            if section not in config:
                config[section] = {}
            config[section][key] = widget.text()

        with open(filename, 'w') as configfile:
            config.write(configfile)

        self.status_box.append(f"{filename} has been updated.")
        dialog.accept()

        # Reload connections after saving
        if filename == 'pg.ini':
            self.pg_connections = self.load_pg_connections('pg.ini')
            self.update_pg_connection_dropdown()
        elif filename == 'neo4j.ini':
            self.neo4j_connections = self.load_neo4j_connections('neo4j.ini')
            self.update_neo4j_connection_dropdown()

    def update_pg_connection_dropdown(self):
        current_text = self.pg_connection_dropdown.currentText()
        self.pg_connection_dropdown.clear()
        self.pg_connection_dropdown.addItems(self.pg_connections.keys())
        if current_text in self.pg_connections:
            self.pg_connection_dropdown.setCurrentText(current_text)
        elif self.pg_connections:
            self.update_pg_fields(next(iter(self.pg_connections)))

    def update_neo4j_connection_dropdown(self):
        current_text = self.neo4j_connection_dropdown.currentText()
        self.neo4j_connection_dropdown.clear()
        self.neo4j_connection_dropdown.addItems(self.neo4j_connections.keys())
        if current_text in self.neo4j_connections:
            self.neo4j_connection_dropdown.setCurrentText(current_text)
        elif self.neo4j_connections:
            self.update_neo4j_fields(next(iter(self.neo4j_connections)))

    def open_file(self):
        self.status_box.append("Open file action triggered")

    def save_file(self):
        self.status_box.append("Save file action triggered")

    def create_postgresql_group(self):
        group = QGroupBox("PostgreSQL")
        layout = QVBoxLayout()

        # Connection dropdown
        self.pg_connection_dropdown = QComboBox()
        self.pg_connection_dropdown.addItems(self.pg_connections.keys())
        self.pg_connection_dropdown.currentTextChanged.connect(self.update_pg_fields)
        layout.addWidget(QLabel("Select Connection:"))
        layout.addWidget(self.pg_connection_dropdown)

        # Input fields
        self.pg_inputs = {}
        for field in ['url', 'user', 'password']:
            layout.addWidget(QLabel(f"{field}:"))
            self.pg_inputs[field] = QLineEdit()
            if field == 'password':
                self.pg_inputs[field].setEchoMode(QLineEdit.Password)
            layout.addWidget(self.pg_inputs[field])

        # Initialize fields with first connection
        if self.pg_connections:
            self.update_pg_fields(next(iter(self.pg_connections)))

        self.pg_test_btn = QPushButton("Test Connection")
        self.pg_test_btn.clicked.connect(self.test_pg_connection)
        layout.addWidget(self.pg_test_btn)
    
        self.pg_table = QTableWidget(10, 5)  # Set initial row count to 10
        self.pg_table.setHorizontalHeaderLabels(["name", "# of columns", "# of rows", "view", "info"])
        self.pg_table.verticalHeader().setDefaultSectionSize(30)  # Adjust row height
        self.pg_table.setMinimumHeight(330)  # Approximate height for 10 rows + header
        layout.addWidget(self.pg_table)

        group.setLayout(layout)
        return group

    def create_neo4j_group(self):
        group = QGroupBox("Neo4j")
        layout = QVBoxLayout()

        # Connection dropdown
        self.neo4j_connection_dropdown = QComboBox()
        self.neo4j_connection_dropdown.addItems(self.neo4j_connections.keys())
        self.neo4j_connection_dropdown.currentTextChanged.connect(self.update_neo4j_fields)
        layout.addWidget(QLabel("Select Connection:"))
        layout.addWidget(self.neo4j_connection_dropdown)

        # Input fields
        self.neo_inputs = {}
        for field in ['url', 'user', 'password']:
            layout.addWidget(QLabel(f"{field}:"))
            self.neo_inputs[field] = QLineEdit()
            if field == 'password':
                self.neo_inputs[field].setEchoMode(QLineEdit.Password)
            layout.addWidget(self.neo_inputs[field])

        # Initialize fields with first connection
        if self.neo4j_connections:
            self.update_neo4j_fields(next(iter(self.neo4j_connections)))

        self.neo_test_btn = QPushButton("Test Connection")
        self.neo_test_btn.clicked.connect(self.test_neo_connection)
        layout.addWidget(self.neo_test_btn)
        
        self.neo_table = QTableWidget(10, 5)  # Set initial row count to 10
        self.neo_table.setHorizontalHeaderLabels(["name", "# of properties", "# of nodes", "view", "info"])
        self.neo_table.verticalHeader().setDefaultSectionSize(30)  # Adjust row height
        self.neo_table.setMinimumHeight(330)  # Approximate height for 10 rows + header
        layout.addWidget(self.neo_table)

        group.setLayout(layout)
        return group
    
    def update_pg_fields(self, connection_name):
        if connection_name in self.pg_connections:
            conn_info = self.pg_connections[connection_name]
            self.pg_inputs['url'].setText(conn_info.get('url', ''))
            self.pg_inputs['user'].setText(conn_info.get('user', ''))
            self.pg_inputs['password'].setText(conn_info.get('password', ''))

    def update_neo4j_fields(self, connection_name):
        if connection_name in self.neo4j_connections:
            conn_info = self.neo4j_connections[connection_name]
            self.neo_inputs['url'].setText(conn_info.get('url', ''))
            self.neo_inputs['user'].setText(conn_info.get('user', ''))
            self.neo_inputs['password'].setText(conn_info.get('password', ''))

    def create_sql_query_group(self):
        group = QGroupBox("SQL Query")
        layout = QVBoxLayout()

        self.sql_edit = QTextEdit()
        self.sql_edit.setPlaceholderText("Enter SQL query here (SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, etc.)")
        layout.addWidget(self.sql_edit)

        query_layout = QHBoxLayout()
        
        self.sql_query_dropdown = QComboBox()
        self.sql_query_dropdown.addItem("Select a saved query")
        self.sql_query_dropdown.addItems(self.saved_sql_queries.keys())
        
        self.sql_query_dropdown.currentTextChanged.connect(self.load_sql_query)
        query_layout.addWidget(self.sql_query_dropdown)

        delete_sql_button = QPushButton("Delete Query")
        delete_sql_button.clicked.connect(self.delete_sql_query)
        query_layout.addWidget(delete_sql_button)

        edit_sql_button = QPushButton("Edit Query")
        edit_sql_button.clicked.connect(self.edit_sql_query)
        query_layout.addWidget(edit_sql_button)

        self.execute_sql_button = QPushButton("Execute SQL")
        self.execute_sql_button.clicked.connect(self.execute_sql_query)
        query_layout.addWidget(self.execute_sql_button)

        layout.addLayout(query_layout)

        group.setLayout(layout)
        return group

    def create_cypher_query_group(self):
        group = QGroupBox("Cypher Query")
        layout = QVBoxLayout()

        self.cypher_edit = QTextEdit()
        self.cypher_edit.setPlaceholderText("Enter Cypher query here")
        layout.addWidget(self.cypher_edit)

        query_layout = QHBoxLayout()
        
        self.cypher_query_dropdown = QComboBox()
        self.cypher_query_dropdown.addItem("Select a saved query")
        self.cypher_query_dropdown.addItems(self.saved_cypher_queries.keys())

        self.cypher_query_dropdown.currentTextChanged.connect(self.load_cypher_query)
        query_layout.addWidget(self.cypher_query_dropdown)

        delete_cypher_button = QPushButton("Delete Query")
        delete_cypher_button.clicked.connect(self.delete_cypher_query)
        query_layout.addWidget(delete_cypher_button)

        edit_cypher_button = QPushButton("Edit Query")
        edit_cypher_button.clicked.connect(self.edit_cypher_query)
        query_layout.addWidget(edit_cypher_button)

        self.execute_cypher_button = QPushButton("Execute Cypher")
        self.execute_cypher_button.clicked.connect(self.execute_cypher_query)
        query_layout.addWidget(self.execute_cypher_button)

        layout.addLayout(query_layout)

        group.setLayout(layout)
        return group

    def edit_sql_query(self):
        query_name = self.sql_query_dropdown.currentText()
        if query_name != "Select a saved query":
            current_query = self.saved_sql_queries[query_name]
            dialog = QueryEditDialog(self, current_query)
            if dialog.exec_():
                edited_query = dialog.get_edited_query()
                self.saved_sql_queries[query_name] = edited_query
                self.save_queries('sql.ini', self.saved_sql_queries)
                self.sql_edit.setText(edited_query)
                self.status_box.append(f"SQL query '{query_name}' updated.")
        else:
            self.status_box.append("Please select a saved query to edit.")

    def edit_cypher_query(self):
        query_name = self.cypher_query_dropdown.currentText()
        if query_name != "Select a saved query":
            current_query = self.saved_cypher_queries[query_name]
            dialog = QueryEditDialog(self, current_query)
            if dialog.exec_():
                edited_query = dialog.get_edited_query()
                self.saved_cypher_queries[query_name] = edited_query
                self.save_queries('cypher.ini', self.saved_cypher_queries)
                self.cypher_edit.setText(edited_query)
                self.status_box.append(f"Cypher query '{query_name}' updated.")
        else:
            self.status_box.append("Please select a saved query to edit.")

    def delete_sql_query(self):
        query_name = self.sql_query_dropdown.currentText()
        if query_name != "Select a saved query":
            self.saved_sql_queries.pop(query_name, None)
            self.sql_query_dropdown.removeItem(self.sql_query_dropdown.currentIndex())
            self.save_queries('sql.ini', self.saved_sql_queries)
            self.sql_edit.clear()

    def delete_cypher_query(self):
        query_name = self.cypher_query_dropdown.currentText()
        if query_name != "Select a saved query":
            self.saved_cypher_queries.pop(query_name, None)
            self.cypher_query_dropdown.removeItem(self.cypher_query_dropdown.currentIndex())
            self.save_queries('cypher.ini', self.saved_cypher_queries)
            self.cypher_edit.clear()   

    def load_sql_query(self, query_name):
        if query_name != "Select a saved query":
            self.sql_edit.setText(self.saved_sql_queries[query_name])

    def load_cypher_query(self, query_name):
        if query_name != "Select a saved query":
            self.cypher_edit.setText(self.saved_cypher_queries[query_name])
                    
    def test_pg_connection(self):
        try:
            # Clear existing contents
            self.pg_table.setRowCount(0)
            self.table_dropdown.clear()
            self.table_dropdown.addItem("Select a table")
            self.status_box.clear()
            self.status_box.append("Testing PostgreSQL connection...")
            QApplication.processEvents()  # Update the UI

            conn = psycopg2.connect(
                f"{self.pg_inputs['url'].text()}?client_encoding=utf8",
                user=self.pg_inputs['user'].text(),
                password=self.pg_inputs['password'].text()
            )

            cur = conn.cursor()

            # Step 1: Get table names and column counts
            self.status_box.append("Fetching table information...")
            QApplication.processEvents()
            cur.execute("""
                SELECT table_name, COUNT(column_name) as column_count
                FROM information_schema.columns
                WHERE table_schema = 'public'
                GROUP BY table_name
                ORDER BY table_name ASC
            """)
            tables_info = cur.fetchall()

            # Step 2: Get row counts for each table
            table_row_counts = {}
            for table_name, _ in tables_info:
                self.status_box.append(f'Counting rows in table: "{table_name}"')
                QApplication.processEvents()
                cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                row_count = cur.fetchone()[0]
                table_row_counts[table_name] = row_count

            # Populate the table widget
            self.status_box.append("Populating table widget...")
            QApplication.processEvents()
            for i, (table_name, column_count) in enumerate(tables_info):
                row_count = table_row_counts.get(table_name, 0)
                self.pg_table.insertRow(i)
                self.pg_table.setItem(i, 0, QTableWidgetItem(table_name))
                self.pg_table.setItem(i, 1, QTableWidgetItem(str(column_count)))
                self.pg_table.setItem(i, 2, QTableWidgetItem(str(row_count)))
                self.table_dropdown.addItem(table_name)
                self.status_box.append(f'Added table: "{table_name}"')
                QApplication.processEvents()

            conn.close()
            self.status_box.append("PostgreSQL connection successful")
        except Exception as e:
            self.status_box.append(f"PostgreSQL connection error: {str(e)}")
            
    def test_neo_connection(self):
        try:
            # Clear existing contents
            self.neo_table.setRowCount(0)
            self.status_box.clear()
            self.status_box.append("Testing Neo4j connection...")
            QApplication.processEvents()  # Update the UI

            driver = GraphDatabase.driver(self.neo_inputs['url'].text(), 
                                        auth=(self.neo_inputs['user'].text(), 
                                                self.neo_inputs['password'].text()))
            
            self.status_box.append("Connection established. Fetching label information...")
            QApplication.processEvents()

            with driver.session() as session:
                result = session.run("""
                    CALL db.labels() YIELD label
                    CALL apoc.cypher.run('MATCH (n:`' + label + '`) 
                                        RETURN count(n) as nodeCount, 
                                                size(apoc.coll.toSet(reduce(s = [], n IN collect(n) | s + keys(n)))) as propCount', 
                                        {}) YIELD value
                    RETURN label, value.nodeCount AS count, value.propCount AS propCount
                    ORDER BY label ASC
                """)
                labels = list(result)
                
                self.status_box.append(f"Found {len(labels)} labels. Populating table...")
                QApplication.processEvents()

                for i, record in enumerate(labels):
                    self.neo_table.insertRow(i)
                    self.neo_table.setItem(i, 0, QTableWidgetItem(record['label']))
                    self.neo_table.setItem(i, 1, QTableWidgetItem(str(record['propCount'])))
                    self.neo_table.setItem(i, 2, QTableWidgetItem(str(record['count'])))
                    self.status_box.append(f"Added label: {record['label']}")
                    QApplication.processEvents()
            
            driver.close()
            self.status_box.append("Neo4j connection successful")
        except Exception as e:
            self.status_box.append(f"Neo4j connection error: {e}")

    def test_neo_connection_v1(self):
        try:
            driver = GraphDatabase.driver(self.neo_inputs['url'].text(), 
                                        auth=(self.neo_inputs['user'].text(), 
                                                self.neo_inputs['password'].text()))
            with driver.session() as session:
                result = session.run("""
                    CALL db.labels() YIELD label
                    CALL apoc.cypher.run('MATCH (n:`' + label + '`) 
                                        WITH count(n) as nodeCount, 
                                            reduce(s = [], k IN keys(n) | s + k) AS allProps
                                        RETURN nodeCount, size(apoc.coll.toSet(allProps)) as propCount', 
                                        {}) YIELD value
                    RETURN label, value.nodeCount AS count, value.propCount AS propCount
                """)
                labels = list(result)
                
                self.neo_table.setRowCount(len(labels))
                for i, record in enumerate(labels):
                    self.neo_table.setItem(i, 0, QTableWidgetItem(record['label']))
                    self.neo_table.setItem(i, 1, QTableWidgetItem(str(record['propCount'])))
                    self.neo_table.setItem(i, 2, QTableWidgetItem(str(record['count'])))
            
            driver.close()
            self.status_box.append("Neo4j connection successful")
        except Exception as e:
            self.status_box.append(f"Neo4j connection error: {e}")

    def execute_sql_query(self):
        query = self.sql_edit.toPlainText()
        if query.strip():
            selected_query = self.sql_query_dropdown.currentText()
            if selected_query != "Select a saved query":
                # Overwrite existing query
                self.saved_sql_queries[selected_query] = query
            else:
                # Create a new unnamed query
                unnamed_count = sum(1 for name in self.saved_sql_queries.keys() if name.startswith("Unnamed Query"))
                new_name = f"Unnamed Query {unnamed_count + 1}"
                self.saved_sql_queries[new_name] = query
                self.sql_query_dropdown.addItem(new_name)
            
            self.save_queries('sql.ini', self.saved_sql_queries)

        try:
            conn = psycopg2.connect(
                f"{self.pg_inputs['url'].text()}?client_encoding=utf8",
                user=self.pg_inputs['user'].text(),
                password=self.pg_inputs['password'].text()
            )

            cur = conn.cursor()
            
            # Function to quote identifiers
            def quote_identifier(identifier):
                return f'"{identifier}"'
            
            # Function to replace unquoted identifiers with quoted ones
            def replace_identifiers(match):
                return quote_identifier(match.group(0))
            
            # Regular expression to match unquoted identifiers
            identifier_pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b(?=\s*\.|\s+(?:from|join|update|into)\s+|\s*$)'
            
            # Replace unquoted identifiers in the query
            quoted_query = re.sub(identifier_pattern, replace_identifiers, query, flags=re.IGNORECASE)
            
            # Check if the query is a SELECT statement
            is_select = quoted_query.strip().upper().startswith("SELECT")
            
            cur.execute(quoted_query)
            
            if is_select:
                result = cur.fetchall()
                # Display the result in the status box
                self.status_box.append("SQL Query Result:")
                for row in result:
                    self.status_box.append(str(row))
            else:
                # For non-SELECT queries, commit the changes and show affected rows
                conn.commit()
                self.status_box.append(f"SQL Query executed successfully. Rows affected: {cur.rowcount}")
            
            conn.close()
        except Exception as e:
            self.status_box.append(f"SQL Query Error: {str(e)}")

    def execute_sql_query_v1(self):
        query = self.sql_edit.toPlainText()
        if query.strip():
            query_name, ok = QInputDialog.getText(self, "Save Query", "Enter a name for this query:")
            if ok and query_name:
                self.saved_sql_queries[query_name] = query
                self.sql_query_dropdown.addItem(query_name)
                self.save_queries('sql.ini', self.saved_sql_queries)

        try:
            conn = psycopg2.connect(
                f"{self.pg_inputs['url'].text()}?client_encoding=utf8",
                user=self.pg_inputs['user'].text(),
                password=self.pg_inputs['password'].text()
            )

            cur = conn.cursor()
            
            # Function to quote identifiers
            def quote_identifier(identifier):
                return f'"{identifier}"'
            
            # Function to replace unquoted identifiers with quoted ones
            def replace_identifiers(match):
                return quote_identifier(match.group(0))
            
            # Regular expression to match unquoted identifiers
            identifier_pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b(?=\s*\.|\s+(?:from|join|update|into)\s+|\s*$)'
            
            # Replace unquoted identifiers in the query
            quoted_query = re.sub(identifier_pattern, replace_identifiers, query, flags=re.IGNORECASE)
            
            # Check if the query is a SELECT statement
            is_select = quoted_query.strip().upper().startswith("SELECT")
            
            cur.execute(quoted_query)
            
            if is_select:
                result = cur.fetchall()
                # Display the result in the status box
                self.status_box.append("SQL Query Result:")
                for row in result:
                    self.status_box.append(str(row))
            else:
                # For non-SELECT queries, commit the changes and show affected rows
                conn.commit()
                self.status_box.append(f"SQL Query executed successfully. Rows affected: {cur.rowcount}")
            
            conn.close()
        except Exception as e:
            self.status_box.append(f"SQL Query Error: {str(e)}")

    def execute_cypher_query(self):
        query = self.cypher_edit.toPlainText()
        if query.strip():
                query_name, ok = QInputDialog.getText(self, "Save Query", "Enter a name for this query:")
                if ok and query_name:
                    self.saved_cypher_queries[query_name] = query
                    self.cypher_query_dropdown.addItem(query_name)
                    self.save_queries('cypher.ini', self.saved_cypher_queries)

        try:
            driver = GraphDatabase.driver(self.neo_inputs['url'].text(), 
                                          auth=(self.neo_inputs['user'].text(), 
                                                self.neo_inputs['password'].text()))
            with driver.session() as session:
                result = session.run(query)
                
                # Check if the query returns results
                summary = result.consume()
                if summary.counters.contains_updates:
                    # For queries that modify the database
                    self.status_box.append("Cypher Query Result:")
                    self.status_box.append(f"Nodes created: {summary.counters.nodes_created}")
                    self.status_box.append(f"Nodes deleted: {summary.counters.nodes_deleted}")
                    self.status_box.append(f"Relationships created: {summary.counters.relationships_created}")
                    self.status_box.append(f"Relationships deleted: {summary.counters.relationships_deleted}")
                    self.status_box.append(f"Properties set: {summary.counters.properties_set}")
                else:
                    # For queries that return data
                    records = list(result)
                    self.status_box.append("Cypher Query Result:")
                    for record in records:
                        self.status_box.append(str(record))

            driver.close()
        except Exception as e:
            self.status_box.append(f"Cypher Query Error: {str(e)}")

    def migrate_data(self, specific_table=None):
        self.status_box.append("Starting migration...")
        self.progress_bar.setValue(0)
        try:
            # PostgreSQL connection
            # conn = psycopg2.connect(self.pg_inputs['url'].text(),
            #                         user=self.pg_inputs['user'].text(),
            #                         password=self.pg_inputs['password'].text())
            pg_conn = psycopg2.connect(
                f"{self.pg_inputs['url'].text()}?client_encoding=utf8",
                user=self.pg_inputs['user'].text(),
                password=self.pg_inputs['password'].text()
            )

            pg_cur = pg_conn.cursor()

            # Neo4j connection
            neo4j_driver = GraphDatabase.driver(self.neo_inputs['url'].text(), 
                                                auth=(self.neo_inputs['user'].text(), 
                                                      self.neo_inputs['password'].text()))

            # Get tables to migrate
            if specific_table:
                tables = [(specific_table,)]
            else:
                pg_cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                """)
                tables = pg_cur.fetchall()

            total_tables = len(tables)
            table_progress_step = 100 // total_tables if total_tables > 0 else 100

            with neo4j_driver.session() as neo4j_session:
                for table_index, table in enumerate(tables):
                    table_name = table[0]
                    self.status_box.append(f'Migrating table: "{table_name}"')

                    # Get data from PostgreSQL
                    pg_cur.execute(f'SELECT * FROM "{table_name}"')
                    rows = pg_cur.fetchall()

                    # Get column names
                    pg_cur.execute(f'SELECT column_name FROM information_schema.columns WHERE table_name = %s', (table_name,))
                    columns = [col[0] for col in pg_cur.fetchall()]

                    # Migrate data to Neo4j
                    total_rows = len(rows)
                    update_interval = max(1, total_rows // 10)  # Ensure we don't divide by zero

                    for row_index, row in enumerate(rows):
                        try:
                            properties = dict(zip(columns, row))
                            cypher_query = f'CREATE (n:`{table_name}` $properties)'
                            neo4j_session.run(cypher_query, properties=properties)

                            # Update progress
                            row_progress = (row_index + 1) / total_rows
                            overall_progress = (table_index * table_progress_step) + (row_progress * table_progress_step)
                            self.progress_bar.setValue(int(overall_progress))
                            
                            # Update status at regular intervals
                            if (row_index + 1) % update_interval == 0 or row_index == total_rows - 1:
                                self.status_box.append(f'Migrated {row_index + 1}/{total_rows} rows from "{table_name}"')
                            
                            # Process events to keep the UI responsive
                            QApplication.processEvents()
                        except Exception as row_error:
                            self.status_box.append(f'Error migrating row {row_index + 1} from "{table_name}": {str(row_error)}')
                            # Optionally, you can choose to continue with the next row or break the loop

                    self.status_box.append(f'Completed migrating {total_rows} rows from "{table_name}"')
            pg_conn.close()
            neo4j_driver.close()
            self.progress_bar.setValue(100)
            self.status_box.append("Migration completed successfully.")
        except Exception as e:
            self.status_box.append(f"Migration error: {str(e)}")
            print(e)
            self.progress_bar.setValue(0)           

    def migrate_selected_table(self):
        selected_table = self.table_dropdown.currentText()
        if selected_table == "Select a table":
            self.status_box.append("Please select a table to migrate.")
            return
        self.migrate_data(selected_table)

    def migrate_all_tables(self):
        self.migrate_data()

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = DatabaseMigrationGUI()
    sys.exit(app.exec_())