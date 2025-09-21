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
    # run through matches and update elo
    cursor.execute("SELECT * FROM matches ORDER BY match_datetime ASC")
    matches = cursor.fetchall()

    for match in matches:
        match_id, bracket, team1_id, team2_id, team2_won, *scores = match

        # Get current ELOs (or insert age-based defaults)
        for team_id in (team1_id, team2_id):
            cursor.execute("SELECT elo FROM team_elo WHERE teamId = ?", (team_id,))
            if cursor.fetchone() is None:
                # Get team age to calculate starting ELO
                cursor.execute("SELECT teamAge FROM teams WHERE teamId = ?", (team_id,))
                age_result = cursor.fetchone()
                team_age = age_result[0] if age_result else None
                
                starting_elo = get_starting_elo(team_age)
                cursor.execute("INSERT INTO team_elo (teamId, elo) VALUES (?, ?)", (team_id, starting_elo))
                # print(f"Created ELO for team {team_id} (age {team_age}): {starting_elo}")

        cursor.execute("SELECT elo FROM team_elo WHERE teamId = ?", (team1_id,))
        elo1 = cursor.fetchone()[0]
        cursor.execute("SELECT elo FROM team_elo WHERE teamId = ?", (team2_id,))
        elo2 = cursor.fetchone()[0]

        result1 = 0.0 if team2_won else 1.0
        new_elo1, new_elo2 = update_elo(elo1, elo2, result1)

        # Update DB
        cursor.execute("UPDATE team_elo SET elo = ? WHERE teamId = ?", (new_elo1, team1_id))
        cursor.execute("UPDATE team_elo SET elo = ? WHERE teamId = ?", (new_elo2, team2_id))

    conn.commit()

create_elo_table()
process_matches()
report_elo_rankings()

conn.close()