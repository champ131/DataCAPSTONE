# Apply mapping transformations based on the mapping logic for customer data
import pandas as pd

# Load your customer data into a DataFrame





df_customer_cleaned = df_customer.copy()

# Apply name formatting
df_customer_cleaned['FIRST_NAME'] = df_customer_cleaned['FIRST_NAME'].str.title()
df_customer_cleaned['MIDDLE_NAME'] = df_customer_cleaned['MIDDLE_NAME'].str.lower()
df_customer_cleaned['LAST_NAME'] = df_customer_cleaned['LAST_NAME'].str.title()

# Rename columns to match target schema
df_customer_cleaned = df_customer_cleaned.rename(columns={
    "SSN": "SSN",
    "FIRST_NAME": "FIRST_NAME",
    "MIDDLE_NAME": "MIDDLE_NAME",
    "LAST_NAME": "LAST_NAME",
    "CREDIT_CARD_NO": "CREDIT_CARD_NO",
    "APT_NO": "APT_NO",
    "STREET_NAME": "STREET_NAME",
    "CUST_CITY": "CUST_CITY",
    "CUST_STATE": "CUST_STATE",
    "CUST_COUNTRY": "CUST_COUNTRY",
    "CUST_ZIP": "CUST_ZIP",
    "CUST_PHONE": "CUST_PHONE",
    "CUST_EMAIL": "CUST_EMAIL",
    "LAST_UPDATED": "LAST_UPDATED"
})

# Convert columns to appropriate data types
df_customer_cleaned['SSN'] = df_customer_cleaned['SSN'].astype(int)
df_customer_cleaned['CUST_PHONE'] = df_customer_cleaned['CUST_PHONE'].astype(str)

print(df_customer_cleaned.head())  # to preview first few rows







import sys
import pymysql
from pymysql import MySQLError
import re
import logging
import matplotlib.pyplot as plt

# Configure logging
datefmt = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=datefmt)
logger = logging.getLogger(__name__)

# ANSI color codes for green text
class Colors:
    GREEN = '\033[92m'
    RESET = '\033[0m'  # Reset to default color
    
def print_green(text):
    """Print text in green color"""
    print(f"{Colors.GREEN}{text}{Colors.RESET}")

# Fields allowed for customer updates
ALLOWED_CUSTOMER_FIELDS = [
    'FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'CREDIT_CARD_NO',
    'FULL_STREET_ADDRESS', 'CUST_CITY', 'CUST_STATE',
    'CUST_COUNTRY', 'CUST_ZIP', 'CUST_PHONE', 'CUST_EMAIL', 'LAST_UPDATED'
]

# Input validation
def get_validated_input(prompt, pattern, error_msg):
    """Prompt until the user provides input matching the regex pattern."""
    while True:
        val = input(prompt).strip()
        if re.fullmatch(pattern, val):
            return val
        print(f"Invalid input: {error_msg}")

# Database connection
def get_db_connection():
    """Establish and return a database connection using pymysql."""
    try:
        conn = pymysql.connect(
            host='127.0.0.1', port=3306,
            user='root', password='password',
            db='bank_data', cursorclass=pymysql.cursors.Cursor
        )
        logger.info("Connected to bank_data database.")
        return conn
    except MySQLError as e:
        logger.error(f"Database connection failed: {e}")
        sys.exit(1)

# Pretty-print results
def print_results(rows, headers=None):
    if not rows:
        print_green("No records found.\n")
        return
    cols = headers or [f"Col{i}" for i in range(len(rows[0]))]
    widths = [len(h) for h in cols]
    for row in rows:
        for i, item in enumerate(row):
            widths[i] = max(widths[i], len(str(item)))
    header_line = " | ".join(h.ljust(widths[i]) for i, h in enumerate(cols))
    sep_line = "-+-".join('-'*widths[i] for i in range(len(cols)))
    print_green(header_line)
    print_green(sep_line)
    for row in rows:
        print_green(" | ".join(str(item).ljust(widths[i]) for i, item in enumerate(row)))
    print()

# Transaction Details Module
def transaction_details_module(conn):
    zip_code = get_validated_input("Enter Zip Code (5 digits): ", r"\d{5}", "ZIP must be 5 digits")
    month = get_validated_input("Enter Month (MM): ", r"0[1-9]|1[0-2]", "01-12")
    year = get_validated_input("Enter Year (YYYY): ", r"\d{4}", "4-digit year")
    pattern = f"{year}{month}%"
    sql = (
        "SELECT TRANSACTION_ID, CUST_CC_NO, TIMEID, CUST_SSN, "
        "BRANCH_CODE, TRANSACTION_TYPE, TRANSACTION_VALUE "
        "FROM cleaned_cdw_sapp_credit_card "
        "WHERE TIMEID LIKE %s AND CUST_SSN IN "
        "(SELECT SSN FROM cleaned_cdw_sapp_customer1 WHERE CUST_ZIP=%s) "
        "ORDER BY TIMEID DESC;"
    )
    with conn.cursor() as cur:
        cur.execute(sql, (pattern, zip_code))
        rows = cur.fetchall()
        headers = [desc[0] for desc in cur.description]
    print_results(rows, headers)

# Customer Details Module
def view_customer_info(conn):
    ssn = get_validated_input("Enter Customer SSN (9 digits): ", r"\d{9}", "SSN must be 9 digits")
    sql = "SELECT * FROM cleaned_cdw_sapp_customer1 WHERE SSN=%s;"
    with conn.cursor() as cur:
        cur.execute(sql, (ssn,))
        rows = cur.fetchall()
        headers = [desc[0] for desc in cur.description]
    print_results(rows, headers)

def modify_customer_info(conn):
    ssn = get_validated_input("Enter Customer SSN to modify (9 digits): ", r"\d{9}", "SSN must be 9 digits")
    view_customer_info(conn)
    field = input(f"Field to update {ALLOWED_CUSTOMER_FIELDS}: ").strip()
    if field not in ALLOWED_CUSTOMER_FIELDS:
        print("Invalid field.")
        return
    new = input(f"New value for {field}: ").strip()
    sql = f"UPDATE cleaned_cdw_sapp_customer1 SET {field}=%s WHERE SSN=%s;"
    with conn.cursor() as cur:
        cur.execute(sql, (new, ssn))
        updated = cur.rowcount
    if updated:
        conn.commit()
        print(f"Updated {updated} record(s).\n")
    else:
        print("No records updated.\n")
    view_customer_info(conn)

def customer_details_module(conn):
    while True:
        print_green("\nCustomer Details Module:\n1. View Customer Info\n2. Modify Customer Info\n3. Generate Monthly Bill\n4. List Transactions Between Dates\n5. Back to Main Menu")
        choice = input("Select an option: ").strip()
        if choice == '1': view_customer_info(conn)
        elif choice == '2': modify_customer_info(conn)
        elif choice == '3': generate_monthly_bill(conn)
        elif choice == '4': list_customer_transactions_between_dates(conn)
        else: break

# Billing & History Modules
def generate_monthly_bill(conn):
    ssn = get_validated_input("Enter Customer SSN (9 digits): ", r"\d{9}", "SSN must be 9 digits")
    month = get_validated_input("Enter Month (MM): ", r"0[1-9]|1[0-2]", "01-12")
    year = get_validated_input("Enter Year (YYYY): ", r"\d{4}", "4-digit year")
    pattern = f"{year}{month}%"
    sql = "SELECT SUM(TRANSACTION_VALUE) FROM cleaned_cdw_sapp_credit_card WHERE CUST_SSN=%s AND TIMEID LIKE %s;"
    with conn.cursor() as cur:
        cur.execute(sql, (ssn, pattern))
        total = cur.fetchone()[0] or 0
    print_green(f"Total spend for {month}/{year}: ${total:.2f}\n")

def list_customer_transactions_between_dates(conn):
    ssn = get_validated_input("Enter Customer SSN (9 digits): ", r"\d{9}", "SSN must be 9 digits")
    start = get_validated_input("Enter Start Date (YYYYMMDD): ", r"\d{8}", "YYYYMMDD format")
    end = get_validated_input("Enter End Date (YYYYMMDD): ", r"\d{8}", "YYYYMMDD format")
    sql = (
        "SELECT TRANSACTION_ID, CUST_CC_NO, TIMEID, BRANCH_CODE, TRANSACTION_TYPE, TRANSACTION_VALUE "
        "FROM cleaned_cdw_sapp_credit_card "
        "WHERE CUST_SSN=%s AND TIMEID BETWEEN %s AND %s ORDER BY TIMEID DESC;"
    )
    with conn.cursor() as cur:
        cur.execute(sql, (ssn, start, end))
        rows = cur.fetchall()
        headers = [desc[0] for desc in cur.description]
    print_results(rows, headers)

# Loan Application Module (FIXED NAME)
def loan_application_module(conn):
    """Menu to view or look up loan applications."""
    while True:
        print_green("\nLoan Application Module:")
        print_green("1. View All Applications")
        print_green("2. View Application by Application_ID")
        print_green("3. Back to Main Menu")
        choice = input("Select an option: ").strip()
        if choice == '1':
            sql = (
                "SELECT Application_ID, Gender, Married, Dependents, Education, "
                "Self_Employed, Credit_History, Property_Area, Income, Application_Status "
                "FROM loans;"
            )
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                headers = [desc[0] for desc in cur.description]
            print_results(rows, headers)
        elif choice == '2':
            app_id = input("Enter Application_ID (e.g. LP001002): ").strip()
            sql = (
                "SELECT Application_ID, Gender, Married, Dependents, Education, "
                "Self_Employed, Credit_History, Property_Area, Income, Application_Status "
                "FROM loans WHERE Application_ID=%s;"
            )
            with conn.cursor() as cur:
                cur.execute(sql, (app_id,))
                rows = cur.fetchall()
                headers = [desc[0] for desc in cur.description]
            if rows:
                print_results(rows, headers)
            else:
                print_green(f"No application found for Application_ID '{app_id}'.\n")
        else:
            break

# Simple diagnostic function
def loan_data_visualization_module_diagnostic(conn):
    """Simple diagnostic to check income data"""
    
    try:
        print("=== SIMPLE DIAGNOSTIC ===")
        
        # Check if Income column exists and get some sample values
        print("1. Sample Income values:")
        with conn.cursor() as cur:
            cur.execute("SELECT Income FROM loans WHERE Income IS NOT NULL LIMIT 5;")
            income_samples = cur.fetchall()
            for i, row in enumerate(income_samples):
                print(f"   Income sample {i+1}: {row[0]}")
        
        print("\n2. Income statistics:")
        with conn.cursor() as cur:
            cur.execute("SELECT MIN(Income), MAX(Income), AVG(Income) FROM loans WHERE Income IS NOT NULL;")
            stats = cur.fetchone()
            print(f"   Min Income: {stats[0]}")
            print(f"   Max Income: {stats[1]}")
            print(f"   Avg Income: {stats[2]}")
        
        print("\n3. Education and Income sample:")
        with conn.cursor() as cur:
            cur.execute("SELECT Education, Income FROM loans WHERE Income IS NOT NULL LIMIT 3;")
            edu_income = cur.fetchall()
            for row in edu_income:
                print(f"   {row[0]}: {row[1]}")
                
        print("\n4. Test the scaling query:")
        with conn.cursor() as cur:
            cur.execute("SELECT Education, AVG(Income * 1000) AS scaled_avg FROM loans WHERE Income IS NOT NULL GROUP BY Education;")
            scaled_results = cur.fetchall()
            for row in scaled_results:
                print(f"   {row[0]}: {row[1]}")
                
    except Exception as e:
        print(f"Diagnostic error: {e}")
        
        # If Income column doesn't exist, let's see what columns do exist
        print("\nFallback - checking all columns:")
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW COLUMNS FROM loans;")
                columns = cur.fetchall()
                print("Available columns:")
                for col in columns:
                    print(f"   - {col[0]}")
        except Exception as e2:
            print(f"Could not get columns: {e2}")

# MAIN Loan Data Visualization Module
def loan_data_visualization_module(conn):
    """Plot:
    5.1 approval rate by self-employment status
    5.2 number of applications by property area
    5.3 average income by education level (FIXED: changed from LoanAmount to Income)
    """
    try:
        # 5.1 Approval rate by self-employment status
        query1 = (
            "SELECT Self_Employed, "
            "SUM(CASE WHEN Application_Status='Y' THEN 1 ELSE 0 END) AS approved, "
            "COUNT(*) AS total "
            "FROM loans "
            "GROUP BY Self_Employed;"
        )
        with conn.cursor() as cur:
            cur.execute(query1)
            rows1 = cur.fetchall()
        
        if rows1:
            labels = ['Self-Employed' if r[0].strip().lower()=='yes' else 'Not Self-Employed' for r in rows1]
            rates = [(r[1]/r[2])*100 if r[2] else 0 for r in rows1]
            plt.figure()
            colors = ['gold', 'black'] if len(rates) == 2 else ['gold'] * len(rates)
            plt.bar(labels, rates, color=colors)
            plt.title('% Loan Approval Rate by Self-Employment Status')
            plt.xlabel('Self-Employed Status')
            plt.ylabel('Approval Rate (%)')
            plt.tight_layout()
            plt.show()

        # 5.2 Number of applications by property area
        query2 = (
            "SELECT Property_Area, COUNT(*) AS count "
            "FROM loans "
            "GROUP BY Property_Area;"
        )
        with conn.cursor() as cur:
            cur.execute(query2)
            rows2 = cur.fetchall()
        
        if rows2:
            areas, counts2 = zip(*rows2)
            plt.figure()
            colors = ['gold', 'black', '#FFD700'] if len(areas) == 3 else ['gold', 'black'] * (len(areas) // 2 + 1)
            plt.bar(areas, counts2, color=colors[:len(areas)])
            plt.title('Loan Applications by Property Area')
            plt.xlabel('Property Area')
            plt.ylabel('Number of Applications')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()

        # 5.3 Average Income by Education Level (FIXED: convert text to numbers)
        query3 = (
            "SELECT Education, AVG("
            "CASE "
            "WHEN Income = 'low' THEN 25000 "
            "WHEN Income = 'medium' THEN 50000 "
            "WHEN Income = 'high' THEN 100000 "
            "ELSE 50000 "
            "END) AS avg_income "
            "FROM loans "
            "WHERE Income IS NOT NULL "
            "GROUP BY Education;"
        )
        
        with conn.cursor() as cur:
            cur.execute(query3)
            rows3 = cur.fetchall()
        
        if rows3:
            edus, avg_incomes = zip(*rows3)
            plt.figure()
            colors = ['gold', 'black'] if len(edus) == 2 else ['gold', 'black'] * (len(edus) // 2 + 1)
            plt.bar(edus, avg_incomes, color=colors[:len(edus)])
            plt.title('Average Income by Education Level')
            plt.xlabel('Education Level')
            plt.ylabel('Average Income ($)')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()
        else:
            print("No data found for income by education level")
            
    except Exception as e:
        print(f"Error in loan data visualization: {e}")
        print("Checking available columns in loans table...")
        
        # Debug: Show available columns
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW COLUMNS FROM loans;")
                columns = cur.fetchall()
                print("Available columns in 'loans' table:")
                for col in columns:
                    print(f"- {col[0]}")
        except Exception as debug_e:
            print(f"Could not retrieve table schema: {debug_e}")

# Credit-Card Analysis Module
def credit_card_analysis_module(conn):
    """Produce three bar charts:
    1. Transaction counts by type
    2. Top 10 states by number of customers
    3. Top 10 customers by total spend
    """
    # 3.1 Transaction counts by type
    with conn.cursor() as cur:
        cur.execute(
            "SELECT TRANSACTION_TYPE, COUNT(*) FROM cleaned_cdw_sapp_credit_card GROUP BY TRANSACTION_TYPE;"
        )
        data = cur.fetchall()
    types, counts = zip(*data)
    plt.figure()
    colors = ['gold', 'black'] * (len(types) // 2 + 1)
    plt.bar(types, counts, color=colors[:len(types)])
    plt.title('Transaction Counts by Type')
    plt.xlabel('Transaction Type')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    # 3.2 Top 10 states by number of customers
    with conn.cursor() as cur:
        cur.execute(
            "SELECT CUST_STATE, COUNT(*) AS cnt FROM cleaned_cdw_sapp_customer1 "
            "GROUP BY CUST_STATE ORDER BY cnt DESC LIMIT 10;"
        )
        state_data = cur.fetchall()
    states, state_counts = zip(*state_data)
    plt.figure()
    colors = ['gold', 'black'] * (len(states) // 2 + 1)
    plt.bar(states, state_counts, color=colors[:len(states)])
    plt.title('Top 10 States by Number of Customers')
    plt.xlabel('State')
    plt.ylabel('Number of Customers')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    # 3.3 Top 10 customers by total spend
    with conn.cursor() as cur:
        cur.execute(
            "SELECT CONCAT(c.FIRST_NAME, ' ', c.LAST_NAME) AS name, "
            "SUM(t.TRANSACTION_VALUE) AS total_spend "
            "FROM cleaned_cdw_sapp_credit_card t "
            "JOIN cleaned_cdw_sapp_customer1 c ON t.CUST_SSN = c.SSN "
            "GROUP BY name ORDER BY total_spend DESC LIMIT 10;"
        )
        cust_data = cur.fetchall()
    names, spends = zip(*cust_data)
    plt.figure()
    colors = ['gold', 'black'] * (len(names) // 2 + 1)
    plt.bar(names, spends, color=colors[:len(names)])
    plt.title('Top 10 Customers by Total Spend')
    plt.xlabel('Customer')
    plt.ylabel('Total Spend')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

# Main CLI
def main():
    conn = get_db_connection()
    try:
        while True:
            print_green("\nMain Menu:\n1. Transaction Details Module\n2. Customer Details Module\n3. Loan Application Module\n4. Loan Data Visualization Module\n5. Credit-Card Analysis Module\n6. Exit")
            choice = input("Select an option: ").strip()
            if choice == '1':
                transaction_details_module(conn)
            elif choice == '2':
                customer_details_module(conn)
            elif choice == '3':
                loan_application_module(conn)
            elif choice == '4':
                loan_data_visualization_module(conn)  # Fixed: back to main function
            elif choice == '5':
                credit_card_analysis_module(conn)
            elif choice == '6':
                print_green("Exiting application.")
                break
            else:
                print_green("Invalid choice, please select 1-6.")
    finally:
        conn.close()
        print_green("Database connection closed.")

# ----- Test Cases -----
import unittest

class ImportTestCase(unittest.TestCase):
    def test_imports(self):
        try:
            import pymysql, re, logging, matplotlib
        except ModuleNotFoundError as e:
            self.skipTest(f"Missing dependency: {e.name}")

class PrintResultsTestCase(unittest.TestCase):
    def test_print_results_empty(self):
        print_results([], None)

    def test_print_results_basic(self):
        print_results([(1, 2)], ['A', 'B'])

class LoanModuleTestCase(unittest.TestCase):
    def test_loan_module_exists(self):
        self.assertTrue(callable(loan_application_module))

class LoanVizTestCase(unittest.TestCase):
    def test_loan_data_visualization_exists(self):
        self.assertTrue(callable(loan_data_visualization_module))

if __name__ == '__main__':
    main()