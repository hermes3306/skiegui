import sys
import os
import configparser
import psycopg2
import csv
from PyQt6.QtWidgets import (QApplication, QMainWindow, QComboBox, QTableWidget, 
                             QVBoxLayout, QHBoxLayout, QWidget, QTextEdit, 
                             QLabel, QStatusBar, QMenuBar, QMenu, QTableWidgetItem,
                             QHeaderView, QPushButton, QFileDialog, QMessageBox,
                             QPlainTextEdit, QDialog, QSizePolicy)
from PyQt6.QtGui import QAction, QColor, QBrush, QFont, QTextCharFormat, QSyntaxHighlighter, QPainter
from PyQt6.QtCore import Qt, QRegularExpression, QRect, QSize

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
        self.setGeometry(200, 200, 800, 600)

        layout = QVBoxLayout()

        # File info
        file_size = os.path.getsize(self.file_path)
        file_info = f"File: {os.path.basename(self.file_path)} | Size: {file_size} bytes"
        info_label = QLabel(file_info)
        layout.addWidget(info_label)

        # Text editor
        self.editor = QPlainTextEdit()
        self.editor.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        font = QFont("Courier", 10)
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

        # Highlight header (first line)
        header_format = QTextCharFormat()
        header_format.setFontWeight(QFont.Weight.Bold)
        header_format.setForeground(QColor("blue"))
        self.highlight_rules.append(("^.+$", header_format))

        # Highlight quotation marks
        quote_format = QTextCharFormat()
        quote_format.setForeground(QColor("red"))
        self.highlight_rules.append(('"[^"]*"', quote_format))

        # Highlight numbers
        number_format = QTextCharFormat()
        number_format.setForeground(QColor("green"))
        self.highlight_rules.append(("\\b\\d+(\\.\\d+)?\\b", number_format))

    def highlightBlock(self, text):
        for pattern, format in self.highlight_rules:
            expression = QRegularExpression(pattern)
            matches = expression.globalMatch(text)
            while matches.hasNext():
                match = matches.next()
                self.setFormat(match.capturedStart(), match.capturedLength(), format)

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

        # Database info
        self.db_info_label = QLabel("Database: Not connected")
        main_layout.addWidget(self.db_info_label)

        # Table selection and CSV buttons
        table_select_layout = QHBoxLayout()
        table_select_layout.addWidget(QLabel("Select Table:"))
        self.table_combo = QComboBox()
        self.table_combo.currentTextChanged.connect(self.load_table_data)
        table_select_layout.addWidget(self.table_combo)
        
        self.download_csv_btn = QPushButton("Download CSV")
        self.download_csv_btn.clicked.connect(self.download_csv_clicked)
        table_select_layout.addWidget(self.download_csv_btn)

                # Add "View CSV" button
        self.view_csv_btn = QPushButton("View CSV")
        self.view_csv_btn.clicked.connect(self.view_csv)
        table_select_layout.addWidget(self.view_csv_btn)

        self.upload_csv_btn = QPushButton("Upload CSV")
        self.upload_csv_btn.clicked.connect(self.upload_csv)
        table_select_layout.addWidget(self.upload_csv_btn)

        main_layout.addLayout(table_select_layout)

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
        main_layout.addWidget(self.table_widget)

        # Log messages
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMaximumHeight(100)
        main_layout.addWidget(self.log_text)

        main_widget.setLayout(main_layout)
        self.setCentralWidget(main_widget)

        # Status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)



    def connect_to_db(self):
        self.config = configparser.ConfigParser()
        self.config.read('db.ini')

        try:
            self.conn = psycopg2.connect(
                host=self.config['postgresql']['host'],
                port=self.config['postgresql']['port'],
                database=self.config['postgresql']['database'],
                user=self.config['postgresql']['user'],
                password=self.config['postgresql']['password']
            )
            self.cur = self.conn.cursor()

            self.db_info_label.setText(f"Database: {self.config['postgresql']['database']} on {self.config['postgresql']['host']}:{self.config['postgresql']['port']}\n"
                                       f"User: {self.config['postgresql']['user']} | Password: {'*' * len(self.config['postgresql']['password'])}")
            self.update_status("Connected to database", "green")

            self.log_message("Connected to database successfully")
            self.load_tables()
        except Exception as e:
            self.log_message(f"Error connecting to database: {str(e)}")
            self.update_status("Database connection failed", "red")

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
                csv_writer.writerows(rows)

            self.log_message(f"CSV file saved: {file_path}")
            self.update_status(f"CSV downloaded: {table_name}", "#A0A0A0")
            self.current_csv_file = file_path
            return True
        except Exception as e:
            self.log_message(f"Error saving CSV: {str(e)}")
            self.update_status("Error saving CSV", "red")
            return False

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
                if tables:
                    self.table_combo.setCurrentIndex(0)
                    self.log_message(f"Set current table to: {self.table_combo.currentText()}")
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
            
    def upload_csv(self):
        table_name = self.table_combo.currentText()
        if not table_name:
            self.log_message("No table selected")
            return

        file_name, _ = QFileDialog.getOpenFileName(self, "Open CSV", "", "CSV Files (*.csv)")
        if file_name:
            try:
                with open(file_name, 'r') as csvfile:
                    csv_reader = csv.reader(csvfile)
                    headers = next(csv_reader)
                    
                    # Prepare the INSERT statement
                    placeholders = ', '.join(['%s'] * len(headers))
                    insert_query = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({placeholders})"
                    
                    # Insert the data
                    self.cur.executemany(insert_query, csv_reader)
                    self.conn.commit()

                self.log_message(f"CSV file uploaded to table: {table_name}")
                self.update_status(f"CSV uploaded: {table_name}", "#A0A0A0")
                self.load_table_data(table_name)  # Refresh the table view
            except Exception as e:
                self.conn.rollback()
                self.log_message(f"Error uploading CSV: {str(e)}")
                self.update_status("Error uploading CSV", "red")

    def log_message(self, message):
        self.log_text.append(message)

    def update_status(self, message, color):
        self.status_bar.showMessage(message, 5000)
        self.status_bar.setStyleSheet(f"background-color: {color}; color: white;")

    def closeEvent(self, event):
        if self.conn:
            self.conn.close()
            self.log_message("Database connection closed")
        event.accept()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    viewer = DatabaseViewer()
    viewer.show()
    sys.exit(app.exec())