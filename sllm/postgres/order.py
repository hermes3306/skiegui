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
        email VARCHAR(100) UNIQUE NOT NULL
    )
''')

cur.execute('''
    CREATE TABLE IF NOT EXISTS products (
        product_id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        price DECIMAL(10, 2) NOT NULL
    )
''')

cur.execute('''
    CREATE TABLE IF NOT EXISTS orders (
        order_id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES users(user_id),
        order_date TIMESTAMP NOT NULL
    )
''')

cur.execute('''
    CREATE TABLE IF NOT EXISTS order_items (
        order_item_id SERIAL PRIMARY KEY,
        order_id INTEGER REFERENCES orders(order_id),
        product_id INTEGER REFERENCES products(product_id),
        quantity INTEGER NOT NULL
    )
''')

conn.commit()

# Generate random data
users = [(f'user{i}', f'user{i}@example.com') for i in range(1, 101)]
products = [(f'Product {i}', round(random.uniform(10, 1000), 2)) for i in range(1, 101)]

# Insert users
execute_values(cur, "INSERT INTO users (username, email) VALUES %s", users)

# Insert products
execute_values(cur, "INSERT INTO products (name, price) VALUES %s", products)

# Insert orders and order items
for _ in range(100):
    user_id = random.randint(1, 100)
    order_date = datetime.now() - timedelta(days=random.randint(0, 365))
    cur.execute("INSERT INTO orders (user_id, order_date) VALUES (%s, %s) RETURNING order_id",
                (user_id, order_date))
    order_id = cur.fetchone()[0]
    
    for _ in range(random.randint(1, 5)):
        product_id = random.randint(1, 100)
        quantity = random.randint(1, 10)
        cur.execute("INSERT INTO order_items (order_id, product_id, quantity) VALUES (%s, %s, %s)",
                    (order_id, product_id, quantity))

conn.commit()

# Fetch and display data
query = '''
    SELECT o.order_id, u.username, p.name as product_name, oi.quantity, o.order_date
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