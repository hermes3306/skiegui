import sys
import networkx as nx
from PyQt6.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QWidget, QLabel, QDialog
from PyQt6.QtCore import Qt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
from u import DraggableGraph  # Import the DraggableGraph class from util

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Graph Viewer")
        self.setGeometry(100, 100, 800, 600)

        self.G = nx.DiGraph()
        self.G.add_node("A", label="Person", name="Alice")
        self.G.add_node("B", label="Person", name="Bob")
        self.G.add_edge("A", "B")

        self.pos = nx.spring_layout(self.G)
        self.initUI()

    def initUI(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        fig = Figure()
        self.canvas = FigureCanvas(fig)
        self.ax = fig.add_subplot(111)

        layout.addWidget(self.canvas)

        self.draggable_graph = DraggableGraph(fig, self.ax, self.G, self.pos)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
