import redis
import json
import random
import string
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import configparser
import psycopg2
from pymongo import MongoClient
from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator
from neo4j import GraphDatabase
import oracledb

class OrderManagementApp(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title("Order Management System")
        self.geometry("800x600")

        self.create_menu()
        self.create_database_selector()
        self.create_table()
        self.create_log_area()

        # Initialize the database connection
        self.db = None
        self.connect_to_database()

        self.refresh_table()

    def create_menu(self):
        menubar = tk.Menu(self)
        self.config(menu=menubar)

        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="Refresh", command=self.refresh_table)
        file_menu.add_command(label="Exit", command=self.quit)

        order_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Order", menu=order_menu)
        order_menu.add_command(label="Create Order", command=self.create_order)
        order_menu.add_command(label="Read Order", command=self.read_order)
        order_menu.add_command(label="Update Order", command=self.update_order)
        order_menu.add_command(label="Delete Order", command=self.delete_order)
        order_menu.add_command(label="Generate 100 Random Orders", command=self.generate_random_orders)

    def create_database_selector(self):
        self.db_var = tk.StringVar()
        self.db_selector = ttk.Combobox(self, textvariable=self.db_var, 
                                        values=["Redis", "PostgreSQL", "MongoDB", "Couchbase", "Neo4j", "Oracle"])
        self.db_selector.set("Redis")
        self.db_selector.pack(pady=10)
        self.db_selector.bind("<<ComboboxSelected>>", self.on_database_change)

    def on_database_change(self, event):
        self.connect_to_database()
        self.refresh_table()

    def connect_to_database(self):
        config = configparser.ConfigParser()
        try:
            config.read('database.ini')
        except configparser.Error:
            self.log_message("Error: Could not read database.ini file.")
            return

        db_type = self.db_var.get().lower()

        try:
            if db_type == 'redis':
                self.db = redis.Redis(
                    host='redis-10413.c294.ap-northeast-1-2.ec2.redns.redis-cloud.com',
                    port=10413,
                    password='pYFHBlJvQ9sI8kSyZ7geEwgytrXHsj1H'
                )
            elif db_type == 'postgresql':
                self.db = psycopg2.connect(**config['postgresql'])
            elif db_type == 'mongodb':
                client = MongoClient(config['mongodb']['uri'])
                self.db = client.get_database()
            elif db_type == 'couchbase':
                cluster = Cluster(config['couchbase']['url'], ClusterOptions(
                    PasswordAuthenticator(config['couchbase']['username'], config['couchbase']['password'])
                ))
                self.db = cluster.bucket(config['couchbase']['bucket'])
            elif db_type == 'neo4j':
                self.db = GraphDatabase.driver(config['neo4j']['uri'], 
                                               auth=(config['neo4j']['user'], config['neo4j']['password']))
            elif db_type == 'oracle':
                self.db = oracledb.connect(user=config['oracle']['user'],
                                           password=config['oracle']['password'],
                                           dsn=config['oracle']['dsn'])
            
            self.log_message(f"Connected to {db_type} database.")
        except Exception as e:
            self.log_message(f"Error connecting to {db_type} database: {str(e)}")
            self.db = None

    def create_table(self):
        self.tree = ttk.Treeview(self, columns=("ID", "Customer", "Items"), show="headings")
        self.tree.heading("ID", text="Order ID")
        self.tree.heading("Customer", text="Customer Name")
        self.tree.heading("Items", text="Items")
        self.tree.pack(fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(self, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscroll=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def create_log_area(self):
        self.log_area = tk.Text(self, height=5, state='disabled')
        self.log_area.pack(fill=tk.X, padx=10, pady=10)

    def log_message(self, message):
        self.log_area.config(state='normal')
        self.log_area.insert(tk.END, message + "\n")
        self.log_area.see(tk.END)
        self.log_area.config(state='disabled')

    def refresh_table(self):
        for i in self.tree.get_children():
            self.tree.delete(i)

        if self.db is None:
            self.log_message("No database connection. Please select a database.")
            return

        db_type = self.db_var.get().lower()

        if db_type == 'redis':
            keys = self.db.keys("order:*")
            for key in keys:
                order_id = key.decode('utf-8').split(':')[1]
                order_data = self.db.get(key)
                order = json.loads(order_data)
                items_summary = ", ".join([f"{item['quantity']} x {item['item']}" for item in order['items']])
                self.tree.insert("", "end", values=(order_id, order['customer_name'], items_summary))
        elif db_type == 'postgresql':
            with self.db.cursor() as cursor:
                cursor.execute("SELECT id, customer_name, items FROM orders")
                for row in cursor.fetchall():
                    self.tree.insert("", "end", values=row)
        elif db_type == 'mongodb':
            for order in self.db.orders.find():
                items_summary = ", ".join([f"{item['quantity']} x {item['item']}" for item in order['items']])
                self.tree.insert("", "end", values=(str(order['_id']), order['customer_name'], items_summary))
        elif db_type == 'couchbase':
            query = "SELECT META().id as id, customer_name, ARRAY_AGG({'item': i.item, 'quantity': i.quantity}) as items FROM `default` WHERE type = 'order'"
            for row in self.db.query(query):
                items_summary = ", ".join([f"{item['quantity']} x {item['item']}" for item in row['items']])
                self.tree.insert("", "end", values=(row['id'], row['customer_name'], items_summary))
        elif db_type == 'neo4j':
            with self.db.session() as session:
                result = session.run("MATCH (o:Order) RETURN o.id, o.customer_name, o.items")
                for record in result:
                    items_summary = ", ".join([f"{item['quantity']} x {item['item']}" for item in record['o.items']])
                    self.tree.insert("", "end", values=(record['o.id'], record['o.customer_name'], items_summary))
        elif db_type == 'oracle':
            with self.db.cursor() as cursor:
                cursor.execute("SELECT id, customer_name, items FROM orders")
                for row in cursor.fetchall():
                    self.tree.insert("", "end", values=row)

        self.log_message(f"Refreshed table. Total orders: {len(self.tree.get_children())}")

    def create_order(self):
        order_id = simpledialog.askstring("Create Order", "Enter order ID:")
        if not order_id:
            return

        customer_name = simpledialog.askstring("Create Order", "Enter customer name:")
        if not customer_name:
            return

        order_items = []
        while True:
            item = simpledialog.askstring("Create Order", "Enter item (or cancel to finish):")
            if not item:
                break
            quantity = simpledialog.askinteger("Create Order", f"Enter quantity for {item}:")
            price = simpledialog.askfloat("Create Order", f"Enter price for {item}:")
            order_items.append({"item": item, "quantity": quantity, "price": price})

        order = {
            "customer_name": customer_name,
            "items": order_items
        }

        db_type = self.db_var.get().lower()

        if db_type == 'redis':
            self.db.set(f"order:{order_id}", json.dumps(order))
        elif db_type == 'postgresql':
            with self.db.cursor() as cursor:
                cursor.execute("INSERT INTO orders (id, customer_name, items) VALUES (%s, %s, %s)",
                               (order_id, customer_name, json.dumps(order_items)))
            self.db.commit()
        elif db_type == 'mongodb':
            self.db.orders.insert_one({"_id": order_id, **order})
        elif db_type == 'couchbase':
            self.db.upsert(f"order:{order_id}", order)
        elif db_type == 'neo4j':
            with self.db.session() as session:
                session.run("CREATE (o:Order {id: $id, customer_name: $customer_name, items: $items})",
                            id=order_id, customer_name=customer_name, items=order_items)
        elif db_type == 'oracle':
            with self.db.cursor() as cursor:
                cursor.execute("INSERT INTO orders (id, customer_name, items) VALUES (:1, :2, :3)",
                               (order_id, customer_name, json.dumps(order_items)))
            self.db.commit()

        self.log_message(f"Order {order_id} created successfully.")
        self.refresh_table()

    def read_order(self):
        order_id = simpledialog.askstring("Read Order", "Enter order ID to read:")
        if not order_id:
            return

        db_type = self.db_var.get().lower()
        order = None

        if db_type == 'redis':
            order_data = self.db.get(f"order:{order_id}")
            if order_data:
                order = json.loads(order_data)
        elif db_type == 'postgresql':
            with self.db.cursor() as cursor:
                cursor.execute("SELECT customer_name, items FROM orders WHERE id = %s", (order_id,))
                result = cursor.fetchone()
                if result:
                    order = {"customer_name": result[0], "items": json.loads(result[1])}
        elif db_type == 'mongodb':
            order = self.db.orders.find_one({"_id": order_id})
        elif db_type == 'couchbase':
            result = self.db.get(f"order:{order_id}")
            if result.value:
                order = result.value
        elif db_type == 'neo4j':
            with self.db.session() as session:
                result = session.run("MATCH (o:Order {id: $id}) RETURN o", id=order_id)
                record = result.single()
                if record:
                    order = record['o']
        elif db_type == 'oracle':
            with self.db.cursor() as cursor:
                cursor.execute("SELECT customer_name, items FROM orders WHERE id = :1", (order_id,))
                result = cursor.fetchone()
                if result:
                    order = {"customer_name": result[0], "items": json.loads(result[1])}

        if order:
            items_info = "\n".join([f"  - {item['item']}: {item['quantity']} x ${item['price']}" for item in order['items']])
            messagebox.showinfo("Order Details", 
                                f"Order ID: {order_id}\n"
                                f"Customer Name: {order['customer_name']}\n"
                                f"Items:\n{items_info}")
        else:
            messagebox.showerror("Error", f"Order {order_id} not found.")

    def update_order(self):
        order_id = simpledialog.askstring("Update Order", "Enter order ID to update:")
        if not order_id:
            return

        db_type = self.db_var.get().lower()
        order = None

        # Fetch the existing order
        if db_type == 'redis':
            order_data = self.db.get(f"order:{order_id}")
            if order_data:
                order = json.loads(order_data)
        elif db_type == 'postgresql':
            with self.db.cursor() as cursor:
                cursor.execute("SELECT customer_name, items FROM orders WHERE id = %s", (order_id,))
                result = cursor.fetchone()
                if result:
                    order = {"customer_name": result[0], "items": json.loads(result[1])}
        elif db_type == 'mongodb':
            order = self.db.orders.find_one({"_id": order_id})
        elif db_type == 'couchbase':
            result = self.db.get(f"order:{order_id}")
            if result.value:
                order = result.value
        elif db_type == 'neo4j':
            with self.db.session() as session:
                result = session.run("MATCH (o:Order {id: $id}) RETURN o", id=order_id)
                record = result.single()
                if record:
                    order = record['o']
        elif db_type == 'oracle':
            with self.db.cursor() as cursor:
                cursor.execute("SELECT customer_name, items FROM orders WHERE id = :1", (order_id,))
                result = cursor.fetchone()
                if result:
                    order = {"customer_name": result[0], "items": json.loads(result[1])}

        if not order:
            messagebox.showerror("Error", f"Order {order_id} not found.")
            return

        new_name = simpledialog.askstring("Update Order", "Enter new customer name (or cancel to keep current):", 
                                          initialvalue=order['customer_name'])
        if new_name:
            order['customer_name'] = new_name

        if messagebox.askyesno("Update Order", "Do you want to update items?"):
            order['items'] = []
            while True:
                item = simpledialog.askstring("Update Order", "Enter item (or cancel to finish):")
                if not item:
                    break
                quantity = simpledialog.askinteger("Update Order", f"Enter quantity for {item}:")
                price = simpledialog.askfloat("Update Order", f"Enter price for {item}:")
                order['items'].append({"item": item, "quantity": quantity, "price": price})


        # Update the order in the database
        if db_type == 'redis':
            self.db.set(f"order:{order_id}", json.dumps(order))
        elif db_type == 'postgresql':
            with self.db.cursor() as cursor:
                cursor.execute("UPDATE orders SET customer_name = %s, items = %s WHERE id = %s",
                            (order['customer_name'], json.dumps(order['items']), order_id))
            self.db.commit()
        elif db_type == 'mongodb':
            self.db.orders.update_one({"_id": order_id}, {"$set": order})
        elif db_type == 'couchbase':
            self.db.upsert(f"order:{order_id}", order)
        elif db_type == 'neo4j':
            with self.db.session() as session:
                session.run("MATCH (o:Order {id: $id}) SET o.customer_name = $customer_name, o.items = $items",
                            id=order_id, customer_name=order['customer_name'], items=order['items'])
        elif db_type == 'oracle':
            with self.db.cursor() as cursor:
                cursor.execute("UPDATE orders SET customer_name = :1, items = :2 WHERE id = :3",
                            (order['customer_name'], json.dumps(order['items']), order_id))
            self.db.commit()

        self.log_message(f"Order {order_id} updated successfully.")
        self.refresh_table()

    def delete_order(self):
        order_id = simpledialog.askstring("Delete Order", "Enter order ID to delete:")
        if not order_id:
            return

        db_type = self.db_var.get().lower()

        if db_type == 'redis':
            result = self.db.delete(f"order:{order_id}")
        elif db_type == 'postgresql':
            with self.db.cursor() as cursor:
                cursor.execute("DELETE FROM orders WHERE id = %s", (order_id,))
            self.db.commit()
            result = cursor.rowcount > 0
        elif db_type == 'mongodb':
            result = self.db.orders.delete_one({"_id": order_id})
            result = result.deleted_count > 0
        elif db_type == 'couchbase':
            try:
                self.db.remove(f"order:{order_id}")
                result = True
            except:
                result = False
        elif db_type == 'neo4j':
            with self.db.session() as session:
                result = session.run("MATCH (o:Order {id: $id}) DELETE o", id=order_id)
                result = result.consume().counters.nodes_deleted > 0
        elif db_type == 'oracle':
            with self.db.cursor() as cursor:
                cursor.execute("DELETE FROM orders WHERE id = :1", (order_id,))
            self.db.commit()
            result = cursor.rowcount > 0

        if result:
            self.log_message(f"Order {order_id} deleted successfully.")
            self.refresh_table()
        else:
            messagebox.showerror("Error", f"Order {order_id} not found.")

    def generate_random_orders(self):
        items = ["Pizza", "Burger", "Salad", "Pasta", "Sushi", "Taco", "Sandwich", "Soup", "Steak", "Ice Cream"]
        
        for _ in range(100):
            order_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
            customer_name = f"Customer_{random.randint(1, 1000)}"
            num_items = random.randint(1, 5)
            order_items = []
            
            for _ in range(num_items):
                item = random.choice(items)
                quantity = random.randint(1, 5)
                price = round(random.uniform(5, 30), 2)
                order_items.append({"item": item, "quantity": quantity, "price": price})
            
            order = {
                "customer_name": customer_name,
                "items": order_items
            }
            
            db_type = self.db_var.get().lower()

            if db_type == 'redis':
                self.db.set(f"order:{order_id}", json.dumps(order))
            elif db_type == 'postgresql':
                with self.db.cursor() as cursor:
                    cursor.execute("INSERT INTO orders (id, customer_name, items) VALUES (%s, %s, %s)",
                                   (order_id, customer_name, json.dumps(order_items)))
                self.db.commit()
            elif db_type == 'mongodb':
                self.db.orders.insert_one({"_id": order_id, **order})
            elif db_type == 'couchbase':
                self.db.upsert(f"order:{order_id}", order)
            elif db_type == 'neo4j':
                with self.db.session() as session:
                    session.run("CREATE (o:Order {id: $id, customer_name: $customer_name, items: $items})",
                                id=order_id, customer_name=customer_name, items=order_items)
            elif db_type == 'oracle':
                with self.db.cursor() as cursor:
                    cursor.execute("INSERT INTO orders (id, customer_name, items) VALUES (:1, :2, :3)",
                                   (order_id, customer_name, json.dumps(order_items)))
                self.db.commit()
        
        self.log_message("100 random orders have been generated and added to the database.")
        self.refresh_table()

if __name__ == "__main__":
    app = OrderManagementApp()
    app.mainloop()