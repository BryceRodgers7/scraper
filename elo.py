import sqlite3

# create elo table
conn = sqlite3.connect("vbdatav4.db")
cursor = conn.cursor()

def get_starting_elo(team_age):
    """
    Calculate starting ELO based on team age.
    Age 10 = 1000, Age 11 = 1100, Age 12 = 1200, etc.
    """
    if team_age is None:
        return 1500  # Default for teams without age info
    
    # Age 10 = 1000, Age 11 = 1100, etc.
    starting_elo = 1000 + (team_age - 10) * 100
    
    # Ensure minimum ELO of 1000
    return max(1000, starting_elo)

def create_elo_table():
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS team_elo (
        teamId TEXT PRIMARY KEY,
        elo REAL DEFAULT 1500,
        FOREIGN KEY (teamId) REFERENCES teams(teamId)
    )
    ''')
    conn.commit()
    print("ELO table created")

def delete_elo_table():
    """Delete the team_elo table to allow recalculation"""
    cursor.execute("DROP TABLE IF EXISTS team_elo")
    conn.commit()
    print("ELO table deleted")

# ELO 
def expected_score(rating_a, rating_b):
    return 1 / (1 + 10 ** ((rating_b - rating_a) / 400))

# ELO logic
def update_elo(rating_a, rating_b, result_a, k=32):
    exp_a = expected_score(rating_a, rating_b)
    new_rating_a = rating_a + k * (result_a - exp_a)
    new_rating_b = rating_b + k * ((1 - result_a) - (1 - exp_a))
    return new_rating_a, new_rating_b


def report_elo_rankings():
    # Print all teams and their ELO ratings
    print("\n=== TEAM ELO RATINGS ===")
    cursor.execute('''
    SELECT t.teamName, t.teamAge, te.elo 
    FROM team_elo te 
    JOIN teams t ON te.teamId = t.teamId 
    ORDER BY te.elo DESC
    ''')
    elo_rankings = cursor.fetchall()

    for i, (team_name, team_age, elo) in enumerate(elo_rankings, 1):
        age_str = f"(Age {team_age})" if team_age else "(No Age)"
        print(f"{i:3d}. {team_name:<25} {age_str:<8} {elo:7.1f}")

    print(f"\nTotal teams processed: {len(elo_rankings)}")
    print("ELO calculations complete")


# select matches ordered by match_datetime (oldest first)
# This ensures ELO calculations are processed chronologically

def process_matches():
    """Process all matches and calculate ELO ratings in memory"""
    print("Loading matches and team data...")
    
    # Load all matches ordered by datetime
    cursor.execute("SELECT * FROM matches ORDER BY match_datetime ASC")
    matches = cursor.fetchall()
    
    # Load all teams to get their ages for starting ELO
    cursor.execute("SELECT teamId, teamAge FROM teams")
    teams_data = {row[0]: row[1] for row in cursor.fetchall()}
    
    # Load existing ELO ratings from database
    cursor.execute("SELECT teamId, elo FROM team_elo")
    existing_elos = {row[0]: row[1] for row in cursor.fetchall()}
    
    # Initialize ELO dictionary in memory with existing ratings
    team_elos = existing_elos.copy()
    
    print(f"Found {len(existing_elos)} existing ELO ratings in database")
    print(f"Processing {len(matches)} matches...")
    
    for i, match in enumerate(matches):
        match_id, bracket, team1_id, team2_id, team2_won, *scores = match
        
        # Initialize ELOs for teams if they don't exist (use existing or calculate new)
        for team_id in (team1_id, team2_id):
            if team_id not in team_elos:
                team_age = teams_data.get(team_id)
                starting_elo = get_starting_elo(team_age)
                team_elos[team_id] = starting_elo
                # print(f"Initialized ELO for team {team_id} (age {team_age}): {starting_elo}")
        
        # Get current ELOs
        elo1 = team_elos[team1_id]
        elo2 = team_elos[team2_id]
        
        # Calculate new ELOs
        result1 = 0.0 if team2_won else 1.0
        new_elo1, new_elo2 = update_elo(elo1, elo2, result1)
        
        # Update in-memory ELOs
        team_elos[team1_id] = new_elo1
        team_elos[team2_id] = new_elo2
        
        # Progress indicator
        if (i + 1) % 1000 == 0:
            print(f"Processed {i + 1}/{len(matches)} matches...")
    
    print(f"ELO calculations complete. Calculated ratings for {len(team_elos)} teams.")
    return team_elos

def save_elos_to_database(team_elos):
    """Save calculated ELO ratings to the database"""
    print("Saving ELO ratings to database...")
    
    # Prepare data for batch insert/update
    elo_data = [(team_id, elo) for team_id, elo in team_elos.items()]
    
    # Batch insert or replace all ELO ratings
    cursor.executemany("INSERT OR REPLACE INTO team_elo (teamId, elo) VALUES (?, ?)", elo_data)
    conn.commit()
    
    print(f"Saved {len(elo_data)} ELO ratings to database.")

# Uncomment the line below to delete existing ELO data and start fresh
delete_elo_table()

create_elo_table()
team_elos = process_matches()
save_elos_to_database(team_elos)
report_elo_rankings()

conn.close()