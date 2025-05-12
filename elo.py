import sqlite3

# create elo table
conn = sqlite3.connect("vbdata.db")
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS team_elo (
    teamId TEXT PRIMARY KEY,
    elo REAL DEFAULT 1500,
    FOREIGN KEY (teamId) REFERENCES teams(teamId)
)
''')
conn.commit()

# ELO 
def expected_score(rating_a, rating_b):
    return 1 / (1 + 10 ** ((rating_b - rating_a) / 400))

# ELO logic
def update_elo(rating_a, rating_b, result_a, k=32):
    exp_a = expected_score(rating_a, rating_b)
    new_rating_a = rating_a + k * (result_a - exp_a)
    new_rating_b = rating_b + k * ((1 - result_a) - (1 - exp_a))
    return new_rating_a, new_rating_b


# run through matches and update elo
cursor.execute("SELECT * FROM matches ORDER BY matchId")
matches = cursor.fetchall()

for match in matches:
    match_id, bracket, team1_id, team2_id, team2_won, *scores = match

    # Get current ELOs (or insert defaults)
    for team_id in (team1_id, team2_id):
        cursor.execute("SELECT elo FROM team_elo WHERE teamId = ?", (team_id,))
        if cursor.fetchone() is None:
            cursor.execute("INSERT INTO team_elo (teamId, elo) VALUES (?, ?)", (team_id, 1500))

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
conn.close()
