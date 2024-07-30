import sys
import networkx as nx
from PyQt6.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QWidget, QLabel, QDialog
from PyQt6.QtCore import Qt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt

class DraggableGraph:
    def __init__(self, fig, ax, G, pos):
        self.fig = fig
        self.ax = ax
        self.G = G
        self.pos = pos
        self.dragged_node = None
        self.node_props = {}
        self.cid_press = fig.canvas.mpl_connect('button_press_event', self.on_press)
        self.cid_release = fig.canvas.mpl_connect('button_release_event', self.on_release)
        self.cid_motion = fig.canvas.mpl_connect('motion_notify_event', self.on_motion)
        self.draw_graph()

    def draw_graph(self):
        self.ax.clear()
        self.nodes = nx.draw_networkx_nodes(self.G, self.pos, ax=self.ax, node_size=3000)
        self.edges = nx.draw_networkx_edges(self.G, self.pos, ax=self.ax, arrows=True, arrowsize=20, edge_color='gray', width=1.5)
        self.labels = nx.draw_networkx_labels(self.G, self.pos, ax=self.ax, labels={node: f"{data['label']}\n{data['name']}" for node, data in self.G.nodes(data=True)})
        self.fig.canvas.draw()

    def on_press(self, event):
        if event.inaxes != self.ax: return
        for node in self.G.nodes:
            xy = self.pos[node]
            distance = ((xy[0] - event.xdata) ** 2 + (xy[1] - event.ydata) ** 2) ** 0.5
            if distance < 0.1:
                self.dragged_node = node
                return

    def on_release(self, event):
        self.dragged_node = None

    def on_motion(self, event):
        if self.dragged_node is None or event.inaxes != self.ax: return
        self.pos[self.dragged_node] = (event.xdata, event.ydata)
        self.draw_graph()

    def on_click(self, event):
        if event.inaxes != self.ax: return
        for node in self.G.nodes:
            xy = self.pos[node]
            distance = ((xy[0] - event.xdata) ** 2 + (xy[1] - event.ydata) ** 2) ** 0.5
            if distance < 0.1:
                self.show_node_properties(node)
                return

    def show_node_properties(self, node):
        data = self.G.nodes[node]
        props = f"Node: {node}\n"
        props += "\n".join([f"{key}: {value}" for key, value in data.items()])
        dlg = QDialog()
        dlg.setWindowTitle(f"Properties of {node}")
        layout = QVBoxLayout()
        label = QLabel(props)
        layout.addWidget(label)
        dlg.setLayout(layout)
        dlg.exec()

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

app = QApplication(sys.argv)
window = MainWindow()
window.show()
sys.exit(app.exec())
