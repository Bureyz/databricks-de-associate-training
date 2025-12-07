#!/usr/bin/env python3
"""
Workshop Data Generator for Databricks Data Engineering Training
================================================================

Generates synthetic e-commerce data compatible with AdventureWorks schema for:
1. Data Engineering exercises (COPY INTO, MERGE, Streaming)
2. Multiple ML models:
   - Churn Prediction
   - Customer Lifetime Value (CLV)
   - Cross-Sell / Product Recommendations
   - Next Purchase Prediction

Requirements:
    pip install faker pandas numpy

Output structure:
    workshop/
    ├── main/                       # Initial load (Bronze)
    │   ├── Customers.csv
    │   ├── Product.csv
    │   ├── ProductCategory.csv
    │   ├── SalesOrderHeader.csv
    │   ├── SalesOrderDetail.csv
    │   └── Address.csv
    │
    ├── incremental/                # Batch updates (COPY INTO, MERGE)
    │   ├── batch_001/
    │   │   ├── Customers.csv       # New customers
    │   │   ├── Customers_updates.csv
    │   │   ├── Product.csv         # New products
    │   │   ├── SalesOrderHeader.csv
    │   │   └── SalesOrderDetail.csv
    │   ├── batch_002/
    │   └── batch_003/
    │
    └── stream/                     # Streaming data
        └── orders/
            ├── orders_stream_001.json
            └── ... (20 files)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
import random
import uuid
from pathlib import Path
from faker import Faker

# Initialize Faker with multiple locales
fake_pl = Faker('pl_PL')
fake_en = Faker('en_US')
fake_de = Faker('de_DE')
fake_uk = Faker('en_GB')
Faker.seed(42)

# Seed for reproducibility
np.random.seed(42)
random.seed(42)

# Configuration
OUTPUT_DIR = Path(__file__).parent.parent / "dataset" / "workshop"
NUM_CUSTOMERS = 10000
NUM_PRODUCTS = 500
NUM_ADDRESSES = 12000  # More than customers (some have multiple)
NUM_ORDERS = 50000
NUM_ORDER_ITEMS = 150000
NUM_INCREMENTAL_BATCHES = 3
NUM_STREAM_FILES = 20
ORDERS_PER_STREAM_FILE = 100

# Date ranges
START_DATE = datetime(2022, 1, 1)
END_DATE = datetime(2025, 11, 30)
CUTOFF_DATE = datetime(2025, 9, 1)  # For churn calculation

# AdventureWorks compatible ship methods
SHIP_METHODS = ["CARGO TRANSPORT 5", "OVERNIGHT J-FAST", "XRQ - TRUCK GROUND"]

# Product categories - AdventureWorks compatible
PRODUCT_CATEGORIES = [
    # Parent categories (ParentProductCategoryID = NULL)
    {"ProductCategoryID": 1, "ParentProductCategoryID": None, "Name": "Bikes"},
    {"ProductCategoryID": 2, "ParentProductCategoryID": None, "Name": "Components"},
    {"ProductCategoryID": 3, "ParentProductCategoryID": None, "Name": "Clothing"},
    {"ProductCategoryID": 4, "ParentProductCategoryID": None, "Name": "Accessories"},
    # Subcategories - Bikes
    {"ProductCategoryID": 5, "ParentProductCategoryID": 1, "Name": "Mountain Bikes"},
    {"ProductCategoryID": 6, "ParentProductCategoryID": 1, "Name": "Road Bikes"},
    {"ProductCategoryID": 7, "ParentProductCategoryID": 1, "Name": "Touring Bikes"},
    # Subcategories - Components
    {"ProductCategoryID": 8, "ParentProductCategoryID": 2, "Name": "Handlebars"},
    {"ProductCategoryID": 9, "ParentProductCategoryID": 2, "Name": "Bottom Brackets"},
    {"ProductCategoryID": 10, "ParentProductCategoryID": 2, "Name": "Brakes"},
    {"ProductCategoryID": 11, "ParentProductCategoryID": 2, "Name": "Chains"},
    {"ProductCategoryID": 12, "ParentProductCategoryID": 2, "Name": "Cranksets"},
    {"ProductCategoryID": 13, "ParentProductCategoryID": 2, "Name": "Derailleurs"},
    {"ProductCategoryID": 14, "ParentProductCategoryID": 2, "Name": "Forks"},
    {"ProductCategoryID": 15, "ParentProductCategoryID": 2, "Name": "Headsets"},
    {"ProductCategoryID": 16, "ParentProductCategoryID": 2, "Name": "Mountain Frames"},
    {"ProductCategoryID": 17, "ParentProductCategoryID": 2, "Name": "Pedals"},
    {"ProductCategoryID": 18, "ParentProductCategoryID": 2, "Name": "Road Frames"},
    {"ProductCategoryID": 19, "ParentProductCategoryID": 2, "Name": "Saddles"},
    {"ProductCategoryID": 20, "ParentProductCategoryID": 2, "Name": "Touring Frames"},
    {"ProductCategoryID": 21, "ParentProductCategoryID": 2, "Name": "Wheels"},
    # Subcategories - Clothing
    {"ProductCategoryID": 22, "ParentProductCategoryID": 3, "Name": "Bib-Shorts"},
    {"ProductCategoryID": 23, "ParentProductCategoryID": 3, "Name": "Caps"},
    {"ProductCategoryID": 24, "ParentProductCategoryID": 3, "Name": "Gloves"},
    {"ProductCategoryID": 25, "ParentProductCategoryID": 3, "Name": "Jerseys"},
    {"ProductCategoryID": 26, "ParentProductCategoryID": 3, "Name": "Shorts"},
    {"ProductCategoryID": 27, "ParentProductCategoryID": 3, "Name": "Socks"},
    {"ProductCategoryID": 28, "ParentProductCategoryID": 3, "Name": "Tights"},
    {"ProductCategoryID": 29, "ParentProductCategoryID": 3, "Name": "Vests"},
    # Subcategories - Accessories
    {"ProductCategoryID": 30, "ParentProductCategoryID": 4, "Name": "Bike Racks"},
    {"ProductCategoryID": 31, "ParentProductCategoryID": 4, "Name": "Bike Stands"},
    {"ProductCategoryID": 32, "ParentProductCategoryID": 4, "Name": "Bottles and Cages"},
    {"ProductCategoryID": 33, "ParentProductCategoryID": 4, "Name": "Cleaners"},
    {"ProductCategoryID": 34, "ParentProductCategoryID": 4, "Name": "Fenders"},
    {"ProductCategoryID": 35, "ParentProductCategoryID": 4, "Name": "Helmets"},
    {"ProductCategoryID": 36, "ParentProductCategoryID": 4, "Name": "Hydration Packs"},
    {"ProductCategoryID": 37, "ParentProductCategoryID": 4, "Name": "Lights"},
    {"ProductCategoryID": 38, "ParentProductCategoryID": 4, "Name": "Locks"},
    {"ProductCategoryID": 39, "ParentProductCategoryID": 4, "Name": "Panniers"},
    {"ProductCategoryID": 40, "ParentProductCategoryID": 4, "Name": "Pumps"},
    {"ProductCategoryID": 41, "ParentProductCategoryID": 4, "Name": "Tires and Tubes"},
]

COLORS = ["Black", "White", "Red", "Blue", "Silver", "Yellow", "Grey", "Multi", None]
SIZES = ["S", "M", "L", "XL", "38", "40", "42", "44", "46", "48", "50", "52", "54", "56", "58", "60", "62", None]

PRODUCT_LINES = ["M", "R", "S", "T"]  # Mountain, Road, Sport, Touring
CLASSES = ["H", "M", "L"]  # High, Medium, Low
STYLES = ["U", "M", "W"]  # Universal, Men, Women


def generate_guid():
    """Generate UUID in AdventureWorks format."""
    return str(uuid.uuid4()).upper()


def generate_addresses(num_addresses):
    """Generate addresses in AdventureWorks format."""
    addresses = []
    
    # Distribution: 70% US, 15% UK, 10% Germany, 5% Poland
    locales = [(fake_en, "United States", 0.70), 
               (fake_uk, "United Kingdom", 0.15),
               (fake_de, "Germany", 0.10),
               (fake_pl, "Poland", 0.05)]
    
    for i in range(1, num_addresses + 1):
        # Select locale based on distribution
        r = random.random()
        cumulative = 0
        for fake, country, prob in locales:
            cumulative += prob
            if r <= cumulative:
                break
        
        if country == "United States":
            city = fake.city()
            state = fake.state()
            postal = fake.zipcode()
        elif country == "United Kingdom":
            city = fake.city()
            state = fake.county()
            postal = fake.postcode()
        elif country == "Germany":
            city = fake.city()
            state = random.choice(["Bavaria", "Berlin", "Hamburg", "Hesse", "North Rhine-Westphalia"])
            postal = fake.postcode()
        else:  # Poland
            city = fake.city()
            state = random.choice(["Mazowieckie", "Małopolskie", "Dolnośląskie", "Wielkopolskie", 
                                   "Pomorskie", "Śląskie", "Łódzkie", "Lubelskie"])
            postal = fake.postcode()
        
        address = {
            "AddressID": i,
            "AddressLine1": fake.street_address(),
            "AddressLine2": f"Apt. {random.randint(1, 500)}" if random.random() < 0.2 else None,
            "City": city,
            "StateProvince": state,
            "CountryRegion": country,
            "PostalCode": postal,
            "rowguid": generate_guid(),
            "ModifiedDate": (START_DATE + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d %H:%M:%S.000")
        }
        addresses.append(address)
    
    return addresses


def generate_customers(num_customers, addresses):
    """Generate customers in AdventureWorks format with ML features."""
    customers = []
    
    titles_m = ["Mr.", None]
    titles_f = ["Ms.", "Mrs.", None]
    suffixes = [None, "Jr.", "Sr.", "II", "III"]
    
    for i in range(1, num_customers + 1):
        # Determine locale (matching address distribution)
        r = random.random()
        if r < 0.70:
            fake = fake_en
        elif r < 0.85:
            fake = fake_uk
        elif r < 0.95:
            fake = fake_de
        else:
            fake = fake_pl
        
        # Gender and names
        gender = random.choice(["M", "F"])
        if gender == "M":
            first_name = fake.first_name_male()
            title = random.choice(titles_m)
        else:
            first_name = fake.first_name_female()
            title = random.choice(titles_f)
        
        last_name = fake.last_name()
        middle_name = fake.first_name()[0] + "." if random.random() < 0.4 else None
        
        # Company (30% are B2B customers - higher CLV potential)
        is_b2b = random.random() < 0.30
        company_name = fake.company() if is_b2b else None
        
        # Sales person
        sales_person = f"adventure-works\\{fake_en.first_name().lower()}{random.randint(0, 9)}"
        
        # Email
        email_domain = random.choice(["adventure-works.com", "gmail.com", "outlook.com", "yahoo.com"])
        email = f"{first_name.lower()}{random.randint(0, 99)}@{email_domain}"
        email = email.replace(" ", "").replace("ä", "a").replace("ö", "o").replace("ü", "u")
        email = email.replace("ą", "a").replace("ę", "e").replace("ó", "o").replace("ś", "s")
        email = email.replace("ł", "l").replace("ż", "z").replace("ź", "z").replace("ć", "c").replace("ń", "n")
        
        # Phone - format varies by region
        phone = fake.phone_number()[:15]
        
        # Registration date - important for CLV and Churn
        reg_date = START_DATE + timedelta(days=random.randint(0, (END_DATE - START_DATE).days - 90))
        
        customer = {
            "CustomerID": i,
            "NameStyle": 0,
            "Title": title,
            "FirstName": first_name,
            "MiddleName": middle_name,
            "LastName": last_name,
            "Suffix": random.choice(suffixes) if random.random() < 0.05 else None,
            "CompanyName": company_name,
            "SalesPerson": sales_person,
            "EmailAddress": email,
            "Phone": phone,
            # Password fields (dummy)
            "PasswordHash": "L/Rlwxzp4w7RWmEgXX+/A7cXaePEPcp+KwQhl2fJL7w=",
            "PasswordSalt": fake_en.lexify("????????")[:8] + "=",
            "rowguid": generate_guid(),
            "ModifiedDate": reg_date.strftime("%Y-%m-%d %H:%M:%S.000"),
            # ML helper fields (will be calculated later, not saved to CSV)
            "_is_b2b": is_b2b,
            "_registration_date": reg_date,
            "_address_ids": []
        }
        
        # Assign 1-3 addresses per customer
        num_addr = random.choices([1, 2, 3], weights=[70, 20, 10])[0]
        addr_sample = random.sample(range(1, len(addresses) + 1), min(num_addr, len(addresses)))
        customer["_address_ids"] = addr_sample
        
        customers.append(customer)
    
    return customers


def generate_product_categories():
    """Generate product categories in AdventureWorks format."""
    categories = []
    for cat in PRODUCT_CATEGORIES:
        categories.append({
            "ProductCategoryID": cat["ProductCategoryID"],
            "ParentProductCategoryID": cat["ParentProductCategoryID"],
            "Name": cat["Name"],
            "rowguid": generate_guid(),
            "ModifiedDate": "2002-06-01 00:00:00.000"
        })
    return categories


def generate_products(num_products, categories):
    """Generate products in AdventureWorks format."""
    products = []
    
    # Get subcategories (for product assignment)
    subcategories = [c for c in categories if c["ParentProductCategoryID"] is not None]
    
    # Product naming patterns
    bike_names = ["HL Road", "ML Road", "LL Road", "HL Mountain", "ML Mountain", "LL Mountain", "Touring"]
    component_names = ["HL Fork", "ML Fork", "LL Fork", "HL Crankset", "Front Brakes", "Rear Brakes"]
    clothing_names = ["Sport-100", "Short-Sleeve Classic Jersey", "Long-Sleeve Logo Jersey", "Racing"]
    accessory_names = ["Water Bottle", "Patch Kit", "Touring-Panniers", "HL Headset", "Bike Wash"]
    
    for i in range(1, num_products + 1):
        # Select category and adjust naming/pricing
        category = random.choice(subcategories)
        parent_id = category["ParentProductCategoryID"]
        
        # Price and naming based on category
        if parent_id == 1:  # Bikes
            base_name = random.choice(bike_names)
            base_price = random.uniform(500, 3500)
            product_line = random.choice(["M", "R", "T"])
        elif parent_id == 2:  # Components
            base_name = random.choice(component_names)
            base_price = random.uniform(50, 800)
            product_line = random.choice(["M", "R"])
        elif parent_id == 3:  # Clothing
            base_name = random.choice(clothing_names)
            base_price = random.uniform(20, 150)
            product_line = "S"
        else:  # Accessories
            base_name = random.choice(accessory_names)
            base_price = random.uniform(5, 100)
            product_line = "S"
        
        color = random.choice(COLORS)
        size = random.choice(SIZES) if parent_id in [1, 3] else None  # Bikes and Clothing have sizes
        
        # Build product name
        name_parts = [base_name]
        if color:
            name_parts.append(color)
        if size:
            name_parts.append(size)
        
        product_name = f"{base_name} - {color or 'Standard'}, {size}" if size else f"{base_name} - {color or 'Standard'}"
        if not color and not size:
            product_name = base_name
            
        # Product number
        product_number = f"{random.choice(['FR', 'BK', 'BB', 'HL', 'ML', 'LL', 'SO', 'CA'])}-{chr(random.randint(65, 90))}{random.randint(1, 999):03d}"
        
        cost = base_price * random.uniform(0.4, 0.7)
        weight = round(random.uniform(0.1, 20.0), 2) if parent_id in [1, 2] else None
        
        # Sell dates
        sell_start = START_DATE + timedelta(days=random.randint(0, 365))
        sell_end = None
        discontinued = None
        if random.random() < 0.1:  # 10% discontinued
            discontinued = (END_DATE - timedelta(days=random.randint(60, 365))).strftime("%Y-%m-%d %H:%M:%S.000")
        
        product = {
            "ProductID": 680 + i,  # Start from 680 to match AW pattern
            "Name": product_name,
            "ProductNumber": product_number,
            "Color": color,
            "StandardCost": round(cost, 4),
            "ListPrice": round(base_price, 2),
            "Size": size,
            "Weight": weight,
            "ProductCategoryID": category["ProductCategoryID"],
            "ProductModelID": random.randint(1, 130),
            "SellStartDate": sell_start.strftime("%Y-%m-%d %H:%M:%S.000"),
            "SellEndDate": sell_end,
            "DiscontinuedDate": discontinued,
            "ThumbNailPhoto": "0x47494638396150003100F70000...",  # Truncated for brevity
            "ThumbnailPhotoFileName": "no_image_available_small.gif",
            "rowguid": generate_guid(),
            "ModifiedDate": sell_start.strftime("%Y-%m-%d %H:%M:%S.000"),
            # ML helper - popularity score for cross-sell
            "_popularity": random.uniform(0.1, 1.0),
            "_parent_category": parent_id
        }
        products.append(product)
    
    return products


def generate_orders_and_details(num_orders, num_items, customers, products, addresses):
    """Generate orders and order details with patterns for multiple ML models."""
    orders = []
    order_details = []
    detail_id = 110562  # Start from AW pattern
    
    # Create customer behavior profiles for ML
    customer_profiles = {}
    for c in customers:
        cid = c["CustomerID"]
        
        # Churn probability based on customer type
        if c["_is_b2b"]:
            churn_prob = random.uniform(0.05, 0.15)  # Lower churn for B2B
            order_frequency = random.uniform(3, 12)  # More orders per year
            avg_basket = random.uniform(3, 8)  # Larger baskets
        else:
            churn_prob = random.uniform(0.10, 0.25)
            order_frequency = random.uniform(1, 6)
            avg_basket = random.uniform(1, 4)
        
        will_churn = random.random() < churn_prob
        
        # Cross-sell affinity: preferred categories
        preferred_categories = random.sample([1, 2, 3, 4], k=random.randint(1, 3))
        
        customer_profiles[cid] = {
            "frequency": order_frequency,
            "will_churn": will_churn,
            "last_possible_order": CUTOFF_DATE - timedelta(days=90) if will_churn else END_DATE,
            "avg_basket_size": avg_basket,
            "preferred_categories": preferred_categories,
            "purchase_history": [],  # For next purchase prediction
            "total_spent": 0,
            "order_count": 0
        }
    
    # Distribute orders based on customer profiles
    customer_weights = [customer_profiles[c["CustomerID"]]["frequency"] for c in customers]
    total_weight = sum(customer_weights)
    customer_weights = [w / total_weight for w in customer_weights]
    
    # Generate orders
    order_id = 71774  # Start from AW pattern
    
    for i in range(num_orders):
        # Select customer
        customer = np.random.choice(customers, p=customer_weights)
        cid = customer["CustomerID"]
        profile = customer_profiles[cid]
        
        # Order date - after registration, respecting churn pattern
        reg_date = customer["_registration_date"]
        max_date = min(profile["last_possible_order"], END_DATE)
        
        days_range = (max_date - reg_date).days
        if days_range <= 0:
            days_range = 30
        
        order_date = reg_date + timedelta(days=random.randint(1, days_range))
        
        # Dates
        due_date = order_date + timedelta(days=random.randint(12, 14))
        ship_date = order_date + timedelta(days=random.randint(5, 9))
        
        # Status: 5 = Shipped (complete)
        status = 5
        
        # Online order flag (60% online for B2C, 30% for B2B)
        online_flag = 1 if random.random() < (0.60 if not customer["_is_b2b"] else 0.30) else 0
        
        # Address
        addr_id = random.choice(customer["_address_ids"]) if customer["_address_ids"] else random.randint(1, len(addresses))
        
        # Account number
        account_num = f"10-4020-{cid:06d}"
        
        order = {
            "SalesOrderID": order_id,
            "RevisionNumber": random.randint(1, 3),
            "OrderDate": order_date.strftime("%Y-%m-%d %H:%M:%S.000"),
            "DueDate": due_date.strftime("%Y-%m-%d %H:%M:%S.000"),
            "ShipDate": ship_date.strftime("%Y-%m-%d %H:%M:%S.000"),
            "Status": status,
            "OnlineOrderFlag": online_flag,
            "SalesOrderNumber": f"SO{order_id}",
            "PurchaseOrderNumber": f"PO{random.randint(100000000, 999999999)}",
            "AccountNumber": account_num,
            "CustomerID": cid,
            "ShipToAddressID": addr_id,
            "BillToAddressID": addr_id,
            "ShipMethod": random.choice(SHIP_METHODS),
            "CreditCardApprovalCode": None if online_flag == 0 else f"{random.randint(100000, 999999)}Vi{random.randint(1000, 9999)}",
            "SubTotal": 0,  # Calculated from items
            "TaxAmt": 0,
            "Freight": round(random.uniform(5, 50), 4),
            "TotalDue": 0,
            "Comment": None,
            "rowguid": generate_guid(),
            "ModifiedDate": ship_date.strftime("%Y-%m-%d %H:%M:%S.000"),
            "_customer_id": cid,
            "_order_date": order_date
        }
        orders.append(order)
        order_id += 1
    
    # Calculate items per order
    items_per_order = {}
    for order in orders:
        cid = order["_customer_id"]
        profile = customer_profiles[cid]
        num_items_in_order = max(1, int(np.random.poisson(profile["avg_basket_size"])))
        items_per_order[order["SalesOrderID"]] = num_items_in_order
    
    # Scale to match target total items
    current_total = sum(items_per_order.values())
    scale_factor = num_items / current_total if current_total > 0 else 1
    
    for oid in items_per_order:
        items_per_order[oid] = max(1, int(items_per_order[oid] * scale_factor))
    
    # Generate order details with cross-sell patterns
    for order in orders:
        oid = order["SalesOrderID"]
        cid = order["_customer_id"]
        profile = customer_profiles[cid]
        
        subtotal = 0
        items_in_order = []
        
        for _ in range(items_per_order.get(oid, 1)):
            # Select product with preference for customer's favorite categories
            if random.random() < 0.7 and profile["preferred_categories"]:
                # Select from preferred categories
                preferred_products = [p for p in products if p["_parent_category"] in profile["preferred_categories"]]
                if preferred_products:
                    product = random.choice(preferred_products)
                else:
                    product = random.choice(products)
            else:
                product = random.choice(products)
            
            quantity = random.choices([1, 2, 3, 4, 5, 6], weights=[50, 25, 12, 7, 4, 2])[0]
            unit_price = product["ListPrice"]
            discount = random.choices([0, 0.05, 0.10, 0.15, 0.20], weights=[60, 15, 12, 8, 5])[0]
            line_total = round(quantity * unit_price * (1 - discount), 6)
            
            detail = {
                "SalesOrderID": oid,
                "SalesOrderDetailID": detail_id,
                "OrderQty": quantity,
                "ProductID": product["ProductID"],
                "UnitPrice": round(unit_price, 4),
                "UnitPriceDiscount": discount,
                "LineTotal": line_total,
                "rowguid": generate_guid(),
                "ModifiedDate": order["OrderDate"]
            }
            order_details.append(detail)
            items_in_order.append(product["ProductID"])
            subtotal += line_total
            detail_id += 1
        
        # Update order totals
        order["SubTotal"] = round(subtotal, 4)
        order["TaxAmt"] = round(subtotal * 0.08, 4)  # 8% tax
        order["TotalDue"] = round(subtotal + order["TaxAmt"] + order["Freight"], 4)
        
        # Update customer profile for ML
        profile["purchase_history"].append({
            "order_date": order["_order_date"],
            "products": items_in_order,
            "total": order["TotalDue"]
        })
        profile["total_spent"] += order["TotalDue"]
        profile["order_count"] += 1
    
    # Calculate customer aggregates for ML
    for c in customers:
        cid = c["CustomerID"]
        profile = customer_profiles[cid]
        
        if profile["order_count"] > 0:
            last_order = max(profile["purchase_history"], key=lambda x: x["order_date"])
            c["_last_purchase_date"] = last_order["order_date"]
            c["_total_orders"] = profile["order_count"]
            c["_total_spent"] = profile["total_spent"]
            c["_avg_order_value"] = profile["total_spent"] / profile["order_count"]
            c["_days_since_last_order"] = (CUTOFF_DATE - last_order["order_date"]).days
            c["_churn_flag"] = 1 if c["_days_since_last_order"] > 90 else 0
        else:
            c["_last_purchase_date"] = None
            c["_total_orders"] = 0
            c["_total_spent"] = 0
            c["_avg_order_value"] = 0
            c["_days_since_last_order"] = None
            c["_churn_flag"] = 1  # Never purchased = churned
    
    # Add data quality issues (2% of orders)
    num_issues = int(len(orders) * 0.02)
    for idx in random.sample(range(len(orders)), num_issues):
        issue_type = random.choice(["null_customer", "future_date", "negative_freight"])
        if issue_type == "null_customer":
            orders[idx]["CustomerID"] = None
        elif issue_type == "future_date":
            orders[idx]["OrderDate"] = "2026-06-15 00:00:00.000"
        elif issue_type == "negative_freight":
            orders[idx]["Freight"] = -abs(orders[idx]["Freight"])
    
    return orders, order_details


def generate_incremental_batch(batch_num, customers, products, addresses, base_order_id, base_detail_id):
    """Generate incremental data for batch processing exercises."""
    
    # 1. New Customers (100 per batch)
    new_customers = []
    start_cid = max(c["CustomerID"] for c in customers) + 1 + (batch_num - 1) * 100
    
    for i in range(100):
        fake = random.choice([fake_en, fake_pl, fake_de, fake_uk])
        gender = random.choice(["M", "F"])
        first_name = fake.first_name_male() if gender == "M" else fake.first_name_female()
        last_name = fake.last_name()
        
        new_customers.append({
            "CustomerID": start_cid + i,
            "NameStyle": 0,
            "Title": random.choice(["Mr.", "Ms.", None]),
            "FirstName": first_name,
            "MiddleName": fake.first_name()[0] + "." if random.random() < 0.3 else None,
            "LastName": last_name,
            "Suffix": None,
            "CompanyName": fake.company() if random.random() < 0.2 else None,
            "SalesPerson": f"adventure-works\\{fake_en.first_name().lower()}{random.randint(0, 9)}",
            "EmailAddress": f"{first_name.lower()}{random.randint(0, 99)}@{random.choice(['gmail.com', 'outlook.com'])}",
            "Phone": fake.phone_number()[:15],
            "PasswordHash": "L/Rlwxzp4w7RWmEgXX+/A7cXaePEPcp+KwQhl2fJL7w=",
            "PasswordSalt": fake_en.lexify("????????")[:8] + "=",
            "rowguid": generate_guid(),
            "ModifiedDate": END_DATE.strftime("%Y-%m-%d %H:%M:%S.000")
        })
    
    # 2. Customer Updates (50 per batch) - for MERGE exercises
    customer_updates = []
    for c in random.sample(customers, 50):
        customer_updates.append({
            "CustomerID": c["CustomerID"],
            "NameStyle": c["NameStyle"],
            "Title": c["Title"],
            "FirstName": c["FirstName"],
            "MiddleName": c["MiddleName"],
            "LastName": c["LastName"],
            "Suffix": c["Suffix"],
            "CompanyName": fake_en.company() if random.random() < 0.3 else c["CompanyName"],
            "SalesPerson": c["SalesPerson"],
            "EmailAddress": f"{c['FirstName'].lower()}.{c['LastName'].lower()}{random.randint(100, 999)}@gmail.com",
            "Phone": fake_en.phone_number()[:15],
            "PasswordHash": c["PasswordHash"],
            "PasswordSalt": c["PasswordSalt"],
            "rowguid": c["rowguid"],
            "ModifiedDate": END_DATE.strftime("%Y-%m-%d %H:%M:%S.000")
        })
    
    # 3. New Products (20 per batch)
    new_products = []
    start_pid = max(p["ProductID"] for p in products) + 1 + (batch_num - 1) * 20
    subcats = [c for c in PRODUCT_CATEGORIES if c["ParentProductCategoryID"] is not None]
    
    for i in range(20):
        cat = random.choice(subcats)
        parent_id = cat["ParentProductCategoryID"]
        base_price = random.uniform(50, 500) if parent_id != 1 else random.uniform(500, 2000)
        
        new_products.append({
            "ProductID": start_pid + i,
            "Name": f"New Product {start_pid + i} - {cat['Name']}",
            "ProductNumber": f"NP-{chr(random.randint(65, 90))}{random.randint(100, 999)}",
            "Color": random.choice(COLORS),
            "StandardCost": round(base_price * 0.6, 4),
            "ListPrice": round(base_price, 2),
            "Size": random.choice(SIZES) if parent_id in [1, 3] else None,
            "Weight": round(random.uniform(0.5, 10), 2) if parent_id in [1, 2] else None,
            "ProductCategoryID": cat["ProductCategoryID"],
            "ProductModelID": random.randint(1, 130),
            "SellStartDate": END_DATE.strftime("%Y-%m-%d %H:%M:%S.000"),
            "SellEndDate": None,
            "DiscontinuedDate": None,
            "ThumbNailPhoto": "0x47494638396150003100F70000...",
            "ThumbnailPhotoFileName": "no_image_available_small.gif",
            "rowguid": generate_guid(),
            "ModifiedDate": END_DATE.strftime("%Y-%m-%d %H:%M:%S.000")
        })
    
    # 4. New Orders (500 per batch)
    batch_orders = []
    batch_details = []
    order_id = base_order_id
    detail_id = base_detail_id
    
    all_customers = customers + new_customers
    all_products = products + new_products
    
    for i in range(500):
        customer = random.choice(all_customers)
        order_date = END_DATE - timedelta(days=random.randint(1, 10))
        
        order = {
            "SalesOrderID": order_id,
            "RevisionNumber": 1,
            "OrderDate": order_date.strftime("%Y-%m-%d %H:%M:%S.000"),
            "DueDate": (order_date + timedelta(days=12)).strftime("%Y-%m-%d %H:%M:%S.000"),
            "ShipDate": (order_date + timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S.000"),
            "Status": 5,
            "OnlineOrderFlag": random.randint(0, 1),
            "SalesOrderNumber": f"SO{order_id}",
            "PurchaseOrderNumber": f"PO{random.randint(100000000, 999999999)}",
            "AccountNumber": f"10-4020-{customer['CustomerID']:06d}",
            "CustomerID": customer["CustomerID"],
            "ShipToAddressID": random.randint(1, len(addresses)),
            "BillToAddressID": random.randint(1, len(addresses)),
            "ShipMethod": random.choice(SHIP_METHODS),
            "CreditCardApprovalCode": None,
            "SubTotal": 0,
            "TaxAmt": 0,
            "Freight": round(random.uniform(10, 40), 4),
            "TotalDue": 0,
            "Comment": None,
            "rowguid": generate_guid(),
            "ModifiedDate": order_date.strftime("%Y-%m-%d %H:%M:%S.000")
        }
        
        # Generate items
        subtotal = 0
        for _ in range(random.randint(1, 4)):
            product = random.choice(all_products)
            qty = random.randint(1, 3)
            discount = random.choice([0, 0.05, 0.10])
            line_total = round(qty * product["ListPrice"] * (1 - discount), 6)
            
            batch_details.append({
                "SalesOrderID": order_id,
                "SalesOrderDetailID": detail_id,
                "OrderQty": qty,
                "ProductID": product["ProductID"],
                "UnitPrice": product["ListPrice"],
                "UnitPriceDiscount": discount,
                "LineTotal": line_total,
                "rowguid": generate_guid(),
                "ModifiedDate": order_date.strftime("%Y-%m-%d %H:%M:%S.000")
            })
            subtotal += line_total
            detail_id += 1
        
        order["SubTotal"] = round(subtotal, 4)
        order["TaxAmt"] = round(subtotal * 0.08, 4)
        order["TotalDue"] = round(subtotal + order["TaxAmt"] + order["Freight"], 4)
        batch_orders.append(order)
        order_id += 1
    
    return new_customers, customer_updates, new_products, batch_orders, batch_details


def generate_stream_data(products, file_num, base_order_id, base_detail_id):
    """Generate streaming order data in JSON format."""
    orders = []
    detail_id = base_detail_id
    
    for i in range(ORDERS_PER_STREAM_FILE):
        order_id = base_order_id + i
        order_time = END_DATE + timedelta(hours=file_num, minutes=random.randint(0, 59), seconds=random.randint(0, 59))
        
        product = random.choice(products)
        qty = random.randint(1, 5)
        discount = random.choice([0.0, 0.05, 0.10, 0.15])
        
        order = {
            "SalesOrderID": order_id,
            "SalesOrderDetailID": detail_id,
            "CustomerID": random.randint(1, NUM_CUSTOMERS),
            "ProductID": product["ProductID"],
            "OrderDateTime": order_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "OrderQty": qty,
            "UnitPrice": product["ListPrice"],
            "UnitPriceDiscount": discount,
            "LineTotal": round(qty * product["ListPrice"] * (1 - discount), 2),
            "ShipMethod": random.choice(SHIP_METHODS),
            "Status": "Pending"
        }
        orders.append(order)
        detail_id += 1
    
    return orders, detail_id


def clean_for_csv(data, exclude_keys=None):
    """Remove internal keys (starting with _) before saving."""
    if exclude_keys is None:
        exclude_keys = []
    
    cleaned = []
    for row in data:
        cleaned_row = {k: v for k, v in row.items() if not k.startswith("_") and k not in exclude_keys}
        cleaned.append(cleaned_row)
    return cleaned


def save_data(output_dir, customers, categories, products, orders, order_details, addresses):
    """Save all data to files in AdventureWorks format."""
    
    # Create directories
    main_dir = output_dir / "main"
    incremental_dir = output_dir / "incremental"
    stream_dir = output_dir / "stream" / "orders"
    
    for d in [main_dir, incremental_dir, stream_dir]:
        d.mkdir(parents=True, exist_ok=True)
    
    # Save main data (CSV)
    print("Saving main data...")
    
    # Clean data before saving
    customers_clean = clean_for_csv(customers)
    products_clean = clean_for_csv(products)
    orders_clean = clean_for_csv(orders)
    
    pd.DataFrame(customers_clean).to_csv(main_dir / "Customers.csv", index=False)
    pd.DataFrame(categories).to_csv(main_dir / "ProductCategory.csv", index=False)
    pd.DataFrame(products_clean).to_csv(main_dir / "Product.csv", index=False)
    pd.DataFrame(orders_clean).to_csv(main_dir / "SalesOrderHeader.csv", index=False)
    pd.DataFrame(order_details).to_csv(main_dir / "SalesOrderDetail.csv", index=False)
    pd.DataFrame(addresses).to_csv(main_dir / "Address.csv", index=False)
    
    print(f"  - Customers.csv: {len(customers)} records")
    print(f"  - ProductCategory.csv: {len(categories)} records")
    print(f"  - Product.csv: {len(products)} records")
    print(f"  - SalesOrderHeader.csv: {len(orders)} records")
    print(f"  - SalesOrderDetail.csv: {len(order_details)} records")
    print(f"  - Address.csv: {len(addresses)} records")
    
    # Generate and save incremental batches - organized by table name
    print("\nGenerating incremental data (organized by table)...")
    base_order_id = max(o["SalesOrderID"] for o in orders) + 1
    base_detail_id = max(d["SalesOrderDetailID"] for d in order_details) + 1
    
    # Create table-specific directories
    customers_dir = incremental_dir / "Customers"
    products_dir = incremental_dir / "Product"
    orders_dir = incremental_dir / "SalesOrderHeader"
    details_dir = incremental_dir / "SalesOrderDetail"
    
    for d in [customers_dir, products_dir, orders_dir, details_dir]:
        d.mkdir(exist_ok=True)
    
    all_new_customers = []
    all_customer_updates = []
    all_new_products = []
    all_batch_orders = []
    all_batch_details = []
    
    for batch_num in range(1, NUM_INCREMENTAL_BATCHES + 1):
        new_custs, cust_updates, new_prods, batch_orders, batch_details = generate_incremental_batch(
            batch_num, customers, products, addresses, base_order_id, base_detail_id
        )
        
        # Save Customers - new and updates separately
        pd.DataFrame(new_custs).to_csv(
            customers_dir / f"new_customers_batch_{batch_num:03d}.csv", index=False)
        pd.DataFrame(cust_updates).to_csv(
            customers_dir / f"updated_customers_batch_{batch_num:03d}.csv", index=False)
        
        # Save Products - new products
        pd.DataFrame(new_prods).to_csv(
            products_dir / f"new_products_batch_{batch_num:03d}.csv", index=False)
        
        # Save Orders
        pd.DataFrame(batch_orders).to_csv(
            orders_dir / f"new_orders_batch_{batch_num:03d}.csv", index=False)
        
        # Save Order Details
        pd.DataFrame(batch_details).to_csv(
            details_dir / f"new_order_details_batch_{batch_num:03d}.csv", index=False)
        
        print(f"  - Batch {batch_num:03d}:")
        print(f"      Customers/new_customers_batch_{batch_num:03d}.csv: {len(new_custs)} records")
        print(f"      Customers/updated_customers_batch_{batch_num:03d}.csv: {len(cust_updates)} records")
        print(f"      Product/new_products_batch_{batch_num:03d}.csv: {len(new_prods)} records")
        print(f"      SalesOrderHeader/new_orders_batch_{batch_num:03d}.csv: {len(batch_orders)} records")
        print(f"      SalesOrderDetail/new_order_details_batch_{batch_num:03d}.csv: {len(batch_details)} records")
        
        all_new_customers.extend(new_custs)
        all_customer_updates.extend(cust_updates)
        all_new_products.extend(new_prods)
        all_batch_orders.extend(batch_orders)
        all_batch_details.extend(batch_details)
        
        base_order_id += 500
        base_detail_id += len(batch_details)
    
    # Also create "all batches combined" files for convenience
    print("\n  Creating combined incremental files...")
    pd.DataFrame(all_new_customers).to_csv(customers_dir / "all_new_customers.csv", index=False)
    pd.DataFrame(all_customer_updates).to_csv(customers_dir / "all_updated_customers.csv", index=False)
    pd.DataFrame(all_new_products).to_csv(products_dir / "all_new_products.csv", index=False)
    pd.DataFrame(all_batch_orders).to_csv(orders_dir / "all_new_orders.csv", index=False)
    pd.DataFrame(all_batch_details).to_csv(details_dir / "all_new_order_details.csv", index=False)
    
    # Create some duplicate records for data quality exercises
    print("\n  Creating duplicate records for quality exercises...")
    
    # Duplicate customers (same CustomerID, different data - for deduplication exercises)
    dup_customers = []
    for c in random.sample(all_new_customers, min(20, len(all_new_customers))):
        dup = c.copy()
        dup["EmailAddress"] = f"duplicate_{c['EmailAddress']}"
        dup["ModifiedDate"] = (END_DATE + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S.000")
        dup_customers.append(dup)
    pd.DataFrame(dup_customers).to_csv(customers_dir / "duplicated_customers.csv", index=False)
    
    # Duplicate orders
    dup_orders = []
    for o in random.sample(all_batch_orders, min(30, len(all_batch_orders))):
        dup = o.copy()
        dup["ModifiedDate"] = (END_DATE + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S.000")
        dup_orders.append(dup)
    pd.DataFrame(dup_orders).to_csv(orders_dir / "duplicated_orders.csv", index=False)
    
    print(f"      Customers/duplicated_customers.csv: {len(dup_customers)} records")
    print(f"      SalesOrderHeader/duplicated_orders.csv: {len(dup_orders)} records")
    
    # Generate and save streaming data
    print("\nGenerating streaming data...")
    stream_base_order_id = base_order_id
    stream_base_detail_id = base_detail_id + 10000  # Offset to avoid conflicts
    
    for file_num in range(1, NUM_STREAM_FILES + 1):
        stream_orders, stream_base_detail_id = generate_stream_data(
            products, file_num, stream_base_order_id, stream_base_detail_id
        )
        
        with open(stream_dir / f"orders_stream_{file_num:03d}.json", "w") as f:
            for order in stream_orders:
                f.write(json.dumps(order) + "\n")
        
        stream_base_order_id += ORDERS_PER_STREAM_FILE
    
    print(f"  - {NUM_STREAM_FILES} stream files, {ORDERS_PER_STREAM_FILE} orders each")
    
    # ML Summary
    print("\n" + "="*60)
    print("ML DATASET SUMMARY")
    print("="*60)
    
    churned = sum(1 for c in customers if c.get("_churn_flag", 0) == 1)
    with_orders = sum(1 for c in customers if c.get("_total_orders", 0) > 0)
    
    print(f"\n📊 CHURN PREDICTION:")
    print(f"   Total Customers: {len(customers)}")
    print(f"   With Orders: {with_orders}")
    print(f"   Active: {len(customers) - churned} ({(len(customers) - churned)/len(customers)*100:.1f}%)")
    print(f"   Churned: {churned} ({churned/len(customers)*100:.1f}%)")
    
    print(f"\n💰 CLV (Customer Lifetime Value):")
    total_spent = [c.get("_total_spent", 0) for c in customers if c.get("_total_spent", 0) > 0]
    print(f"   Avg Customer Spend: ${np.mean(total_spent):.2f}")
    print(f"   Median: ${np.median(total_spent):.2f}")
    print(f"   Max: ${max(total_spent):.2f}")
    
    print(f"\n🛒 CROSS-SELL / PRODUCT RECOMMENDATIONS:")
    print(f"   Products: {len(products)}")
    print(f"   Categories: {len([c for c in categories if c['ParentProductCategoryID'] is not None])}")
    print(f"   Avg Items per Order: {len(order_details)/len(orders):.1f}")
    
    print(f"\n📅 NEXT PURCHASE PREDICTION:")
    order_counts = [c.get("_total_orders", 0) for c in customers]
    print(f"   Repeat Customers: {sum(1 for x in order_counts if x > 1)}")
    print(f"   Avg Orders per Customer: {np.mean(order_counts):.1f}")


def main():
    print("="*60)
    print("WORKSHOP DATA GENERATOR")
    print("AdventureWorks Compatible + Multi-ML Dataset")
    print("="*60)
    print()
    
    print("Generating addresses...")
    addresses = generate_addresses(NUM_ADDRESSES)
    
    print("Generating customers...")
    customers = generate_customers(NUM_CUSTOMERS, addresses)
    
    print("Generating product categories...")
    categories = generate_product_categories()
    
    print("Generating products...")
    products = generate_products(NUM_PRODUCTS, categories)
    
    print("Generating orders and details...")
    orders, order_details = generate_orders_and_details(
        NUM_ORDERS, NUM_ORDER_ITEMS, customers, products, addresses
    )
    
    print("\nSaving data...")
    save_data(OUTPUT_DIR, customers, categories, products, orders, order_details, addresses)
    
    print("\n" + "="*60)
    print("DONE!")
    print(f"Data saved to: {OUTPUT_DIR}")
    print("="*60)


if __name__ == "__main__":
    main()
