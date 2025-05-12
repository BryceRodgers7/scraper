import requests
import csv
import time
import sqlite3
import os

# Database initialization
def init_database():
    conn = sqlite3.connect('vbdata.db')
    cursor = conn.cursor()
    
    # Create events table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS events (
        eventId TEXT PRIMARY KEY,
        eventName TEXT,
        location TEXT,
        startDate TEXT
    )
    ''')
    
    # Create divisions table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS divisions (
        divisionId TEXT PRIMARY KEY,
        eventId TEXT,
        divisionName TEXT,
        teamCount INTEGER,
        codeAlias TEXT,
        division_url TEXT,
        FOREIGN KEY (eventId) REFERENCES events(eventId)
    )
    ''')
    
    # Create teams table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS teams (
        teamId TEXT PRIMARY KEY,
        teamName TEXT,
        teamCode TEXT,
        clubId TEXT,
        clubName TEXT
    )
    ''')

    # Create enrollments table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS enrollments (
        teamId TEXT,
        divisionId TEXT,     
        matchesWon INTEGER,
        matchesLost INTEGER,
        setsWon INTEGER,
        setsLost INTEGER,
        finishRank INTEGER,
        overallRank INTEGER,
        matchUrl TEXT,
        FOREIGN KEY (divisionId) REFERENCES divisions(divisionId),
        FOREIGN KEY (teamId) REFERENCES teams(teamId)
    )
    ''')
    
    # Create matches table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS matches (
        matchId INTEGER PRIMARY KEY AUTOINCREMENT,
        bracket TEXT,
        team1_id TEXT,
        team2_id TEXT,
        team2_won BOOLEAN,
        set1_team1_score INTEGER,
        set1_team2_score INTEGER,
        set2_team1_score INTEGER,
        set2_team2_score INTEGER,
        set3_team1_score INTEGER,
        set3_team2_score INTEGER,
        FOREIGN KEY (team1_id) REFERENCES teams(teamId),
        FOREIGN KEY (team2_id) REFERENCES teams(teamId)
    )
    ''')
    
    conn.commit()
    return conn

# Initialize database before starting
conn = init_database()
cursor = conn.cursor()

# example list of event urls:
# event_url = ["https://results.advancedeventsystems.com/api/event/PTAwMDAwMzY3OTY90", "https://results.advancedeventsystems.com/api/event/PTAwMDAwMzY3OTY91", "https://results.advancedeventsystems.com/api/event/PTAwMDAwMzY3OTY92"]

# single event url
event_url = ["https://results.advancedeventsystems.com/api/event/PTAwMDAwMzY3OTY90"]

# getEventData() and getDivisionsForTourney() will populate these two lists automatically
division_urls = []
match_urls = []



# get a list of division urls from an event
def getEventData(urls):
    for url in urls:
        # url = "https://results.advancedeventsystems.com/api/event/PTAwMDAwMzY3OTY90"
        parts = url.split('/')
        eventKey = parts[len(parts)-1]

        print(f'getting event data.. {eventKey}')

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36", 
            "Accept": "application/json, text/plain, */*",
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()  # for JSON responses
            # print(data)

            eventId = data["EventId"]
            eventName = data["Name"]
            location = data["Location"]
            startDate = data["StartDate"]

            # Insert event data
            cursor.execute('''
            INSERT OR REPLACE INTO events (eventId, eventName, location, startDate)
            VALUES (?, ?, ?, ?)
            ''', (eventId, eventName, location, startDate))

            for division in data["Divisions"]:
                divisionId = division["DivisionId"]
                divisionName = division["Name"]
                teamCount = division["TeamCount"]
                codeAlias = division["CodeAlias"]
                division_url = f'https://results.advancedeventsystems.com/odata/{eventKey}/standings(dId={divisionId},cId=null,tIds=[])?$orderby=OverallRank,FinishRank,TeamName,TeamCode'

                # Insert division data
                cursor.execute('''
                INSERT OR REPLACE INTO divisions (divisionId, eventId, divisionName, teamCount, codeAlias, division_url)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', (divisionId, eventId, divisionName, teamCount, codeAlias, division_url))

                print(f"Added division: {divisionName}")

                division_urls.append(division_url)

            conn.commit()
            time.sleep(1)

        else:
            print("Failed:", response.status_code)


def getDivisionsForTourney(urls):
    print("getting division data..")

    for i, url in enumerate(urls):
        parts = url.split('/')
        eventJibberish = parts[4]

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36", 
            "Accept": "application/json, text/plain, */*",
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()

            for team in data["value"]:
                teamId = team["TeamId"]
                teamName = team["TeamName"]
                teamCode = team["TeamCode"]
                clubId = team["Club"]["ClubId"]
                clubName = team["Club"]["Name"]
                divisionId = team["Division"]["DivisionId"]
                matchesWon = team["MatchesWon"]
                matchesLost = team["MatchesLost"]
                setsWon = team["SetsWon"]
                setsLost = team["SetsLost"]
                finishRank = team["FinishRank"]
                overallRank = team["OverallRank"]
                matchUrl = f'https://results.advancedeventsystems.com/api/event/{eventJibberish}/division/{divisionId}/team/{teamId}/schedule/past'

                # Insert team data into teams table
                cursor.execute('''
                INSERT OR REPLACE INTO teams (
                    teamId, teamName, teamCode, clubId, clubName
                ) VALUES (?, ?, ?, ?, ?)
                ''', (
                    teamId, teamName, teamCode, clubId, clubName
                ))

                # Insert enrollment data into enrollments table
                cursor.execute('''
                INSERT OR REPLACE INTO enrollments (
                    teamId, divisionId, matchesWon, matchesLost,
                    setsWon, setsLost, finishRank, overallRank, matchUrl
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    teamId, divisionId, matchesWon, matchesLost,
                    setsWon, setsLost, finishRank, overallRank, matchUrl
                ))

                print(f"Added team: {teamName}")
                print(f"Added enrollment for {teamId} in divisionId {divisionId}")

                match_urls.append(matchUrl)

            conn.commit()
            time.sleep(1)

        else:
            print("Failed:", response.status_code)


# get a list of matches from a team
def getMatchData(urls):
    print(f'getting match data for {len(urls)} matches..')
    
    for i, url in enumerate(urls):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36", 
            "Accept": "application/json, text/plain, */*",
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            print(f'iteration: {i}')
            data = response.json()  # for JSON responses
            # print(data)

            for match in data:
                bracket = match["Play"]["CompleteShortName"]
                first_team = match["Match"]["FirstTeamName"]
                first_team_id = match["Match"]["FirstTeamId"]
                second_team = match["Match"]["SecondTeamName"]
                second_team_id = match["Match"]["SecondTeamId"]
                second_team_won = match["Match"]["SecondTeamWon"]
                
                # Initialize set scores
                set1_team1 = set1_team2 = set2_team1 = set2_team2 = set3_team1 = set3_team2 = None
                
                # Extract set scores
                for i, set_score in enumerate(match["Match"]["Sets"], 1):
                    if i == 1:
                        set1_team1 = set_score["FirstTeamScore"]
                        set1_team2 = set_score["SecondTeamScore"]
                    elif i == 2:
                        set2_team1 = set_score["FirstTeamScore"]
                        set2_team2 = set_score["SecondTeamScore"]
                    elif i == 3:
                        set3_team1 = set_score["FirstTeamScore"]
                        set3_team2 = set_score["SecondTeamScore"]

                # Insert match data
                cursor.execute('''
                INSERT INTO matches (
                    bracket, team1_id, team2_id, team2_won,
                    set1_team1_score, set1_team2_score,
                    set2_team1_score, set2_team2_score,
                    set3_team1_score, set3_team2_score
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    bracket, first_team_id, second_team_id, second_team_won,
                    set1_team1, set1_team2,
                    set2_team1, set2_team2,
                    set3_team1, set3_team2
                ))

                print(f"Added match: {first_team} vs {second_team}")

            conn.commit()
            time.sleep(1)

        else:
            print("Failed:", response.status_code)


# Close the database connection when done
try:
    getEventData(event_url)
    getDivisionsForTourney(division_urls)
    getMatchData(match_urls)
finally:
    conn.close()




