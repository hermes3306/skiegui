import configparser
import random
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values

# Read database configuration
config = configparser.ConfigParser()
config.read('db.ini')

# Connect to the database
conn = psycopg2.connect(
    host=config['postgresql']['host'],
    port=config['postgresql']['port'],
    database=config['postgresql']['database'],
    user=config['postgresql']['user'],
    password=config['postgresql']['password']
)

cur = conn.cursor()

# Create tables
cur.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id SERIAL PRIMARY KEY,
        username VARCHAR(50) UNIQUE NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        date_of_birth DATE,
        registration_date TIMESTAMP NOT NULL,
        last_login TIMESTAMP,
        is_active BOOLEAN DEFAULT TRUE,
        user_type VARCHAR(20) CHECK (user_type IN ('regular', 'premium', 'admin'))
    )
''')

cur.execute('''
    CREATE TABLE IF NOT EXISTS products (
        product_id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        description TEXT,
        price DECIMAL(10, 2) NOT NULL,
        category VARCHAR(50),
        stock_quantity INTEGER NOT NULL,
        manufacturer VARCHAR(100),
        weight DECIMAL(8, 2),
        dimensions VARCHAR(50),
        is_available BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP NOT NULL,
        last_updated TIMESTAMP
    )
''')

cur.execute('''
    CREATE TABLE IF NOT EXISTS orders (
        order_id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES users(user_id),
        order_date TIMESTAMP NOT NULL,
        total_amount DECIMAL(12, 2) NOT NULL,
        status VARCHAR(20) CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')),
        shipping_address TEXT,
        billing_address TEXT,
        payment_method VARCHAR(50),
        shipping_method VARCHAR(50),
        tracking_number VARCHAR(100),
        notes TEXT
    )
''')

cur.execute('''
    CREATE TABLE IF NOT EXISTS order_items (
        order_item_id SERIAL PRIMARY KEY,
        order_id INTEGER REFERENCES orders(order_id),
        product_id INTEGER REFERENCES products(product_id),
        quantity INTEGER NOT NULL,
        unit_price DECIMAL(10, 2) NOT NULL,
        subtotal DECIMAL(12, 2) NOT NULL,
        discount DECIMAL(10, 2) DEFAULT 0,
        tax DECIMAL(10, 2) DEFAULT 0
    )
''')

conn.commit()

# Generate random data
users = [
    (
        f'user{i}',
        f'user{i}@example.com',
        f'First{i}',
        f'Last{i}',
        datetime.now() - timedelta(days=random.randint(7000, 25000)),
        datetime.now() - timedelta(days=random.randint(0, 1000)),
        datetime.now() - timedelta(days=random.randint(0, 30)),
        random.choice([True, False]),
        random.choice(['regular', 'premium', 'admin'])
    ) for i in range(1, 101)
]

products = [
    (
        f'Product {i}',
        f'Description for Product {i}',
        round(random.uniform(10, 1000), 2),
        random.choice(['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Toys']),
        random.randint(0, 1000),
        f'Manufacturer {random.randint(1, 20)}',
        round(random.uniform(0.1, 50), 2),
        f'{random.randint(1, 100)}x{random.randint(1, 100)}x{random.randint(1, 100)}',
        random.choice([True, False]),
        datetime.now() - timedelta(days=random.randint(0, 1000)),
        datetime.now() - timedelta(days=random.randint(0, 30))
    ) for i in range(1, 101)
]

# Insert users
execute_values(cur, """
    INSERT INTO users (username, email, first_name, last_name, date_of_birth, registration_date, last_login, is_active, user_type)
    VALUES %s
""", users)

# Insert products
execute_values(cur, """
    INSERT INTO products (name, description, price, category, stock_quantity, manufacturer, weight, dimensions, is_available, created_at, last_updated)
    VALUES %s
""", products)

# Insert orders and order items
for _ in range(100):
    user_id = random.randint(1, 100)
    order_date = datetime.now() - timedelta(days=random.randint(0, 365))
    total_amount = 0
    status = random.choice(['pending', 'processing', 'shipped', 'delivered', 'cancelled'])
    shipping_address = f"{random.randint(1, 999)} Main St, City {random.randint(1, 50)}, State {random.randint(1, 50)}"
    billing_address = shipping_address if random.choice([True, False]) else f"{random.randint(1, 999)} Side St, City {random.randint(1, 50)}, State {random.randint(1, 50)}"
    payment_method = random.choice(['Credit Card', 'PayPal', 'Bank Transfer'])
    shipping_method = random.choice(['Standard', 'Express', 'Overnight'])
    tracking_number = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=12))
    notes = "Some notes about the order" if random.choice([True, False]) else None

    cur.execute("""
        INSERT INTO orders (user_id, order_date, total_amount, status, shipping_address, billing_address, payment_method, shipping_method, tracking_number, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING order_id
    """, (user_id, order_date, total_amount, status, shipping_address, billing_address, payment_method, shipping_method, tracking_number, notes))
    
    order_id = cur.fetchone()[0]
    
    for _ in range(random.randint(1, 5)):
        product_id = random.randint(1, 100)
        quantity = random.randint(1, 10)
        cur.execute("SELECT price FROM products WHERE product_id = %s", (product_id,))
        unit_price = cur.fetchone()[0]
        subtotal = unit_price * quantity
        discount = round(random.uniform(0, subtotal * 0.2), 2)  # Up to 20% discount
        tax = round(subtotal * 0.1, 2)  # 10% tax
        total_amount += subtotal - discount + tax

        cur.execute("""
            INSERT INTO order_items (order_id, product_id, quantity, unit_price, subtotal, discount, tax)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (order_id, product_id, quantity, unit_price, subtotal, discount, tax))

    cur.execute("UPDATE orders SET total_amount = %s WHERE order_id = %s", (total_amount, order_id))

conn.commit()

# Fetch and display data
query = '''
    SELECT o.order_id, u.username, u.email, p.name as product_name, p.category, 
           oi.quantity, oi.unit_price, oi.subtotal, oi.discount, oi.tax, 
           o.total_amount, o.status, o.order_date
    FROM orders o
    JOIN users u ON o.user_id = u.user_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    ORDER BY o.order_id
    LIMIT 100
'''

df = pd.read_sql_query(query, conn)

print(df)

# Close the database connection
cur.close()
conn.close()