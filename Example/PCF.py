# Databricks notebook source
# Create a Delta table to track the product carbon footprint
spark.sql("CREATE TABLE IF NOT EXISTS product_carbon_footprint (product_id STRING, supplier_id STRING, carbon_emission DOUBLE) USING DELTA")

# Insert data into the Delta table
spark.sql("""
    INSERT INTO product_carbon_footprint
    VALUES ('paper_roll_001', 'supplier_001', 10.5),
           ('paper_roll_002', 'supplier_002', 12.3),
           ('paper_roll_003', 'supplier_003', 9.8)
""")

# Create a Delta table to track the order information
spark.sql("CREATE TABLE IF NOT EXISTS order_info (order_id STRING, product_id STRING, supplier_id STRING, quantity INT) USING DELTA")

# Insert data into the Delta table
spark.sql("""
    INSERT INTO order_info
    VALUES ('order_001', 'paper_roll_001', 'supplier_001', 100),
           ('order_002', 'paper_roll_002', 'supplier_002', 200),
           ('order_003', 'paper_roll_003', 'supplier_003', 150)
""")

# Create a Delta table to track the item lines along the value chain
spark.sql("CREATE TABLE IF NOT EXISTS value_chain (order_id STRING, product_id STRING, supplier_id STRING, process_step STRING, carbon_emission DOUBLE) USING DELTA")

# Insert data into the Delta table
spark.sql("""
    INSERT INTO value_chain
    VALUES ('order_001', 'paper_roll_001', 'supplier_001', 'manufacturing', 5.2),
           ('order_001', 'paper_roll_001', 'supplier_001', 'packaging', 2.3),
           ('order_001', 'paper_roll_001', 'supplier_001', 'shipping', 1.5),
           ('order_002', 'paper_roll_002', 'supplier_002', 'manufacturing', 6.1),
           ('order_002', 'paper_roll_002', 'supplier_002', 'packaging', 2.8),
           ('order_002', 'paper_roll_002', 'supplier_002', 'shipping', 1.9),
           ('order_003', 'paper_roll_003', 'supplier_003', 'manufacturing', 4.8),
           ('order_003', 'paper_roll_003', 'supplier_003', 'packaging', 2.1),
           ('order_003', 'paper_roll_003', 'supplier_003', 'shipping', 1.3)
""")

# Create a Delta table to track the invoices
spark.sql("CREATE TABLE IF NOT EXISTS invoices (order_id STRING, product_id STRING, supplier_id STRING, invoice_number STRING, invoice_amount DOUBLE) USING DELTA")

# Insert data into the Delta table
spark.sql("""
    INSERT INTO invoices
    VALUES ('order_001', 'paper_roll_001', 'supplier_001', 'INV001', 500.0),
           ('order_002', 'paper_roll_002', 'supplier_002', 'INV002', 800.0),
           ('order_003', 'paper_roll_003', 'supplier_003', 'INV003', 600.0)
""")

# Query the Delta tables
df1 = spark.sql("SELECT * FROM product_carbon_footprint")
df2 = spark.sql("SELECT * FROM order_info")
df3 = spark.sql("SELECT * FROM value_chain")
df4 = spark.sql("SELECT * FROM invoices")

display(df1)
display(df2)
display(df3)
display(df4)
