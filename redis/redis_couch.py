import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import json
import random
import string
import redis
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator

class Database:
    def __init__(self, db_type):
        self.db_type = db_type
        if db_type == "redis":
            self.client = redis.Redis(
                host='redis-10413.c294.ap-northeast-1-2.ec2.redns.redis-cloud.com',
                port=10413,
                password='pYFHBlJvQ9sI8kSyZ7geEwgytrXHsj1H'
            )
        elif db_type == "couchbase":
            cluster = Cluster('couchbase://localhost', ClusterOptions(
                PasswordAuthenticator('username', 'password')
            ))
            self.client = cluster.bucket('default').default_collection()
        else:
            raise ValueError("Invalid database type")

    def set(self, key, value):
        if self.db_type == "redis":
            return self.client.set(key, json.dumps(value))
        else:
            return self.client.upsert(key, value)

    def get(self, key):
        if self.db_type == "redis":
            value = self.client.get(key)
            return json.loads(value) if value else None
        else:
            result = self.client.get(key)
            return result.value if result else None

    def delete(self, key):
        if self.db_type == "redis":
            return self.client.delete(key)
        else:
            return self.client.remove(key)

    def keys(self, pattern):
        if self.db_type == "redis":
            return self.client.keys(pattern)
        else:
            query = f"SELECT META().id FROM `default` WHERE META().id LIKE '{pattern}'"
            result = self.client.cluster.query(query)
            return [row['id'] for row in result]

class OrderManagementApp(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title("Order Management System")
        self.geometry("800x600")

        self.db = Database("redis")  # Default to Redis

        self.create_menu()
        self.create_table()
        self.create_log_area()

        self.refresh_table()

    def create_menu(self):
        menubar = tk.Menu(self)
        self.config(menu=menubar)

        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="Switch Database", command=self.switch_database)
        file_menu.add_command(label="Refresh", command=self.refresh_table)
        file_menu.add_command(label="Exit", command=self.quit)

        order_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Order", menu=order_menu)
        order_menu.add_command(label="Create Order", command=self.create_order)
        order_menu.add_command(label="Read Order", command=self.read_order)
        order_menu.add_command(label="Update Order", command=self.update_order)
        order_menu.add_command(label="Delete Order", command=self.delete_order)
        order_menu.add_command(label="Generate 100 Random Orders", command=self.generate_random_orders)

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

    def switch_database(self):
        db_type = simpledialog.askstring("Switch Database", "Enter database type (redis/couchbase):")
        if db_type in ["redis", "couchbase"]:
            self.db = Database(db_type)
            self.log_message(f"Switched to {db_type.capitalize()} database")
            self.refresh_table()
        else:
            messagebox.showerror("Error", "Invalid database type")

    def refresh_table(self):
        for i in self.tree.get_children():
            self.tree.delete(i)

        keys = self.db.keys("order:*")
        for key in keys:
            order_id = key.decode('utf-8').split(':')[1] if isinstance(key, bytes) else key.split(':')[1]
            order = self.db.get(key)
            items_summary = ", ".join([f"{item['quantity']} x {item['item']}" for item in order['items']])
            self.tree.insert("", "end", values=(order_id, order['customer_name'], items_summary))

        self.log_message(f"Refreshed table. Total orders: {len(keys)}")

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

        self.db.set(f"order:{order_id}", order)
        self.log_message(f"Order {order_id} created successfully.")
        self.refresh_table()

    def read_order(self):
        order_id = simpledialog.askstring("Read Order", "Enter order ID to read:")
        if not order_id:
            return

        order = self.db.get(f"order:{order_id}")
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

        order = self.db.get(f"order:{order_id}")
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

        self.db.set(f"order:{order_id}", order)
        self.log_message(f"Order {order_id} updated successfully.")
        self.refresh_table()

    def delete_order(self):
        order_id = simpledialog.askstring("Delete Order", "Enter order ID to delete:")
        if not order_id:
            return

        if self.db.delete(f"order:{order_id}"):
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
            
            self.db.set(f"order:{order_id}", order)
        
        self.log_message("100 random orders have been generated and added to the database.")
        self.refresh_table()

if __name__ == "__main__":
    app = OrderManagementApp()
    app.mainloop()