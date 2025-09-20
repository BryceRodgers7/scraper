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

def export_team_elo_summary(cursor, output_dir):
    """Export joined data showing team ELO ratings with team names and division names"""
    
    # Query to join the four tables
    query = """
    SELECT 
        t.teamId,
        t.teamName,
        t.teamCode,
        t.teamAge,
        t.clubName,
        d.divisionName,
        d.eventId,
        e.matchesWon,
        e.matchesLost,
        e.setsWon,
        e.setsLost,
        e.finishRank,
        e.overallRank,
        te.elo
    FROM teams t
    LEFT JOIN enrollments e ON t.teamId = e.teamId
    LEFT JOIN divisions d ON e.divisionId = d.divisionId
    LEFT JOIN team_elo te ON t.teamId = te.teamId
    ORDER BY te.elo DESC, t.teamName
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    if not rows:
        print("No data found for team ELO summary")
        return
    
    # Define column headers
    columns = [
        'teamId', 'teamName', 'teamCode', 'teamAge', 'clubName', 'divisionName', 
        'eventId', 'matchesWon', 'matchesLost', 'setsWon', 'setsLost', 
        'finishRank', 'overallRank', 'elo'
    ]
    
    # Create CSV filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = os.path.join(output_dir, f"team_elo_summary_{timestamp}.csv")
    
    # Write to CSV
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)  # Write header
        writer.writerows(rows)    # Write data
    
    print(f"Exported {len(rows)} rows from team ELO summary to {csv_filename}")
    
    # Print some statistics
    teams_with_elo = sum(1 for row in rows if row[13] is not None)  # elo column (now index 13)
    teams_without_elo = len(rows) - teams_with_elo
    
    print(f"Teams with ELO ratings: {teams_with_elo}")
    print(f"Teams without ELO ratings: {teams_without_elo}")
    
    if teams_with_elo > 0:
        elo_values = [row[13] for row in rows if row[13] is not None]  # elo column (now index 13)
        print(f"Highest ELO: {max(elo_values):.1f}")
        print(f"Lowest ELO: {min(elo_values):.1f}")
        print(f"Average ELO: {sum(elo_values)/len(elo_values):.1f}")

def main():
    # Create output directory if it doesn't exist
    output_dir = "db_exports_v3"
    os.makedirs(output_dir, exist_ok=True)
    
    # Connect to the database
    conn = sqlite3.connect('vbdatav3.db')
    cursor = conn.cursor()
    
    # Get list of all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [table[0] for table in cursor.fetchall()]
    
    # Export each table
    for table in tables:
        export_table_to_csv(cursor, table, output_dir)
    
    # Export the joined team ELO summary
    print("\n" + "="*50)
    print("EXPORTING TEAM ELO SUMMARY")
    print("="*50)
    export_team_elo_summary(cursor, output_dir)
    
    # Close the connection
    conn.close()
    
    print("\nExport complete! Files are in the 'db_exports_v3' directory.")

if __name__ == "__main__":
    main()
