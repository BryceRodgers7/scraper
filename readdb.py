import sqlite3
import csv
import os
from datetime import datetime

def export_table_to_csv(cursor, table_name, output_dir):
    """Export a single table to a CSV file"""
    # Get all rows from the table
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    
    if not rows:
        print(f"No data found in table {table_name}")
        return
    
    # Get column names
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [column[1] for column in cursor.fetchall()]
    
    # Create CSV filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = os.path.join(output_dir, f"{table_name}_{timestamp}.csv")
    
    # Write to CSV
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)  # Write header
        writer.writerows(rows)    # Write data
    
    print(f"Exported {len(rows)} rows from {table_name} to {csv_filename}")

def main():
    # Create output directory if it doesn't exist
    output_dir = "db_exports"
    os.makedirs(output_dir, exist_ok=True)
    
    # Connect to the database
    conn = sqlite3.connect('vbdata.db')
    cursor = conn.cursor()
    
    # Get list of all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [table[0] for table in cursor.fetchall()]
    
    # Export each table
    for table in tables:
        export_table_to_csv(cursor, table, output_dir)
    
    # Close the connection
    conn.close()
    
    print("\nExport complete! Files are in the 'db_exports' directory.")

if __name__ == "__main__":
    main()
