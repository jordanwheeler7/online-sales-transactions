"""
    Author: Jordan Wheeler
    Date: 2023-10-03
    Description: This is a simple program that will create a csv file utilizing Faker.
    
"""

import random
from faker import Faker
import csv
from datetime import datetime, timedelta

# Initialize Faker with a seed for reproducibility
fake = Faker()
Faker.seed(0)

# Number of transactions to generate
num_transactions = 1000

# Updated list of payment methods
payment_methods = ["Credit Card", "Debit Card", "PayPal", "Apple Pay", "Google Wallet", "Store Card"]

# List of merchandise categories
categories = ["Electronics", "Clothing", "Books", "Home & Garden", "Sports & Outdoors"]

# Output CSV file
output_file = "online_transactions.csv"

# Create a list to store the transactions
transactions = []

# Generate transactions
for _ in range(num_transactions):
    payment_method = random.choice(payment_methods)
    payment_amount = round(random.uniform(10, 500), 2)  # Random payment amount between $10 and $500
    category = random.choice(categories)
    timestamp = fake.date_time_between(start_date="-1y", end_date="now")  # Transactions from the past year

    transactions.append([payment_method, payment_amount, category, timestamp])

# Sort transactions by timestamp
transactions.sort(key=lambda x: x[3])

# Write the data to a CSV file
with open(output_file, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Payment Method", "Payment Amount", "Category", "Timestamp"])
    writer.writerows(transactions)

print(f"Generated {num_transactions} online transactions and saved to {output_file}.")
