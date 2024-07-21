import redis
import json
import random
import string

# Connect to Redis using the provided details
r = redis.Redis(
    host='redis-10413.c294.ap-northeast-1-2.ec2.redns.redis-cloud.com',
    port=10413,
    password='pYFHBlJvQ9sI8kSyZ7geEwgytrXHsj1H'
)

def create_order():
    order_id = input("Enter order ID: ")
    customer_name = input("Enter customer name: ")
    order_items = []
    
    while True:
        item = input("Enter item (or press enter to finish): ")
        if not item:
            break
        quantity = int(input("Enter quantity: "))
        price = float(input("Enter price: "))
        order_items.append({"item": item, "quantity": quantity, "price": price})
    
    order = {
        "customer_name": customer_name,
        "items": order_items
    }
    
    r.set(f"order:{order_id}", json.dumps(order))
    print(f"Order {order_id} created successfully.")

def read_order():
    order_id = input("Enter order ID to read: ")
    order_data = r.get(f"order:{order_id}")
    
    if order_data:
        order = json.loads(order_data)
        print(f"Order ID: {order_id}")
        print(f"Customer Name: {order['customer_name']}")
        print("Items:")
        for item in order['items']:
            print(f"  - {item['item']}: {item['quantity']} x ${item['price']}")
    else:
        print(f"Order {order_id} not found.")

def update_order():
    order_id = input("Enter order ID to update: ")
    order_data = r.get(f"order:{order_id}")
    
    if order_data:
        order = json.loads(order_data)
        print("Current order details:")
        print(f"Customer Name: {order['customer_name']}")
        print("Items:")
        for item in order['items']:
            print(f"  - {item['item']}: {item['quantity']} x ${item['price']}")
        
        # Update customer name
        new_name = input("Enter new customer name (or press enter to keep current): ")
        if new_name:
            order['customer_name'] = new_name
        
        # Update items
        while True:
            action = input("Do you want to add, remove, or update an item? (a/r/u/done): ").lower()
            if action == 'done':
                break
            elif action == 'a':
                item = input("Enter item name: ")
                quantity = int(input("Enter quantity: "))
                price = float(input("Enter price: "))
                order['items'].append({"item": item, "quantity": quantity, "price": price})
            elif action == 'r':
                item = input("Enter item name to remove: ")
                order['items'] = [i for i in order['items'] if i['item'] != item]
            elif action == 'u':
                item = input("Enter item name to update: ")
                for i in order['items']:
                    if i['item'] == item:
                        i['quantity'] = int(input("Enter new quantity: "))
                        i['price'] = float(input("Enter new price: "))
        
        r.set(f"order:{order_id}", json.dumps(order))
        print(f"Order {order_id} updated successfully.")
    else:
        print(f"Order {order_id} not found.")

def delete_order():
    order_id = input("Enter order ID to delete: ")
    if r.delete(f"order:{order_id}"):
        print(f"Order {order_id} deleted successfully.")
    else:
        print(f"Order {order_id} not found.")

def list_orders():
    keys = r.keys("order:*")
    if keys:
        print("List of orders:")
        for key in keys:
            order_id = key.decode('utf-8').split(':')[1]
            order_data = r.get(key)
            order = json.loads(order_data)
            print(f"Order ID: {order_id}, Customer: {order['customer_name']}")
    else:
        print("No orders found.")

def generate_random_orders():
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
        
        r.set(f"order:{order_id}", json.dumps(order))
    
    print("100 random orders have been generated and added to Redis.")

def main_menu():
    while True:
        print("\nOrder Management System")
        print("1. Create Order")
        print("2. Read Order")
        print("3. Update Order")
        print("4. Delete Order")
        print("5. List Orders")
        print("6. Generate 100 Random Orders")
        print("7. Exit")
        
        choice = input("Enter your choice (1-7): ")
        
        if choice == '1':
            create_order()
        elif choice == '2':
            read_order()
        elif choice == '3':
            update_order()
        elif choice == '4':
            delete_order()
        elif choice == '5':
            list_orders()
        elif choice == '6':
            generate_random_orders()
        elif choice == '7':
            print("Thank you for using the Order Management System. Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main_menu()