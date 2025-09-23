import requests
import csv
import time
import sqlite3
import os
import gc
from datetime import datetime

# Database initialization
def init_database():
    conn = sqlite3.connect('vbdatav4.db')
    cursor = conn.cursor()
    
    # Enable WAL mode for better concurrent access and performance
    cursor.execute('PRAGMA journal_mode=WAL')
    
    # Optimize database settings for bulk inserts
    cursor.execute('PRAGMA synchronous=NORMAL')
    cursor.execute('PRAGMA cache_size=10000')
    cursor.execute('PRAGMA temp_store=MEMORY')
    cursor.execute('PRAGMA mmap_size=268435456')  # 256MB memory mapping
    
    # Create indexes for better query performance
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS events (
        eventId TEXT PRIMARY KEY,
        eventName TEXT,
        location TEXT,
        startDate TEXT
    )
    ''')
    
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
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS teams (
        teamId TEXT PRIMARY KEY,
        teamName TEXT,
        teamCode TEXT,
        clubId TEXT,
        clubName TEXT,
        teamAge INTEGER
    )
    ''')
    
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
        match_datetime DATETIME,
        FOREIGN KEY (team1_id) REFERENCES teams(teamId),
        FOREIGN KEY (team2_id) REFERENCES teams(teamId)
    )
    ''')
    
    # Create indexes for better performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_matches_datetime ON matches(match_datetime)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_matches_teams ON matches(team1_id, team2_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_enrollments_team ON enrollments(teamId)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_enrollments_division ON enrollments(divisionId)')
    
    conn.commit()
    return conn

# Initialize database before starting
conn = init_database()
cursor = conn.cursor()

# example list of event urls:
# event_url = ["https://results.advancedeventsystems.com/api/event/PTAwMDAwMzY3OTY90", "https://results.advancedeventsystems.com/api/event/PTAwMDAwMzY3OTY91", "https://results.advancedeventsystems.com/api/event/PTAwMDAwMzY3OTY92"]

# single event url
# event_url = ["https://results.advancedeventsystems.com/api/event/PTAwMDAwMzY3OTY90"]

# getEventData() and getDivisionsForTourney() will populate these two lists automatically
division_urls = []
match_urls = []

event_list = []

def initEventUrls():
    event_list.append('https://www.advancedeventsystems.com/api/landing/events?$count=true&$filter=((startDate+lt+2025-09-01T05:00:00%2B00:00+and+endDate+ge+2024-09-01T05:00:00%2B00:00)+and+isPastEvent+eq+true+and+eventType%2FeventTypeId+eq+5)&$format=json&$orderby=startDate+desc,name&$top=100')
    event_list.append('https://www.advancedeventsystems.com/api/landing/events?$count=true&$filter=((startDate+lt+2025-09-01T05:00:00%2B00:00+and+endDate+ge+2024-09-01T05:00:00%2B00:00)+and+isPastEvent+eq+true+and+eventType%2FeventTypeId+eq+5)&$format=json&$orderby=startDate+desc,name&$skip=100&$top=100')
    event_list.append('https://www.advancedeventsystems.com/api/landing/events?$count=true&$filter=((startDate+lt+2025-09-01T05:00:00%2B00:00+and+endDate+ge+2024-09-01T05:00:00%2B00:00)+and+isPastEvent+eq+true+and+eventType%2FeventTypeId+eq+5)&$format=json&$orderby=startDate+desc,name&$skip=200&$top=100')
    event_list.append('https://www.advancedeventsystems.com/api/landing/events?$count=true&$filter=((startDate+lt+2025-09-01T05:00:00%2B00:00+and+endDate+ge+2024-09-01T05:00:00%2B00:00)+and+isPastEvent+eq+true+and+eventType%2FeventTypeId+eq+5)&$format=json&$orderby=startDate+desc,name&$skip=300&$top=100')
    event_list.append('https://www.advancedeventsystems.com/api/landing/events?$count=true&$filter=((startDate+lt+2025-09-01T05:00:00%2B00:00+and+endDate+ge+2024-09-01T05:00:00%2B00:00)+and+isPastEvent+eq+true+and+eventType%2FeventTypeId+eq+5)&$format=json&$orderby=startDate+desc,name&$skip=400&$top=100')
    event_list.append('https://www.advancedeventsystems.com/api/landing/events?$count=true&$filter=((startDate+lt+2025-09-01T05:00:00%2B00:00+and+endDate+ge+2024-09-01T05:00:00%2B00:00)+and+isPastEvent+eq+true+and+eventType%2FeventTypeId+eq+5)&$format=json&$orderby=startDate+desc,name&$skip=500&$top=100')
    event_list.append('https://www.advancedeventsystems.com/api/landing/events?$count=true&$filter=((startDate+lt+2025-09-01T05:00:00%2B00:00+and+endDate+ge+2024-09-01T05:00:00%2B00:00)+and+isPastEvent+eq+true+and+eventType%2FeventTypeId+eq+5)&$format=json&$orderby=startDate+desc,name&$skip=600&$top=100')
    event_list.append('https://www.advancedeventsystems.com/api/landing/events?$count=true&$filter=((startDate+lt+2025-09-01T05:00:00%2B00:00+and+endDate+ge+2024-09-01T05:00:00%2B00:00)+and+isPastEvent+eq+true+and+eventType%2FeventTypeId+eq+5)&$format=json&$orderby=startDate+desc,name&$skip=700&$top=100')
    event_list.append('https://www.advancedeventsystems.com/api/landing/events?$count=true&$filter=((startDate+lt+2025-09-01T05:00:00%2B00:00+and+endDate+ge+2024-09-01T05:00:00%2B00:00)+and+isPastEvent+eq+true+and+eventType%2FeventTypeId+eq+5)&$format=json&$orderby=startDate+desc,name&$skip=800&$top=100')

def getEventKeys():

    # url = 'https://www.advancedeventsystems.com/api/landing/events?$count=true&$filter=(isSchedulerPosted+eq+true+and+(startDate+lt+2025-09-01T05:00:00%2B00:00+and+endDate+ge+2024-09-01T05:00:00%2B00:00)+and+isPastEvent+eq+true+and+eventType%2FeventTypeId+eq+11)&$format=json&$orderby=startDate+desc,name&$top=100'

    
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36", 
        "Accept": "application/json, text/plain, */*",
    }

    # Fetch all existing event keys from the database
    print("Fetching existing event keys from database...")
    cursor.execute("SELECT eventId FROM events")
    existing_event_ids = set(row[0] for row in cursor.fetchall())
    print(f"Found {len(existing_event_ids)} existing events in database")

    event_urls = []
    
    try:
        for url in event_list:
            print(f"Fetching event data from: {url}")
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract eventSchedulerKey values from the 'value' array
                event_keys = []
                if 'value' in data and isinstance(data['value'], list):
                    for event in data['value']:
                        if 'eventSchedulerKey' in event and event['eventSchedulerKey']:
                            event_keys.append(event['eventSchedulerKey'])
                
                print(f"Total eventSchedulerKey values found: {len(event_keys)}")

                skipped_count = 0
                added_count = 0
                
                for event_key in event_keys:
                    # Check if this event key already exists in the database
                    if event_key in existing_event_ids:
                        skipped_count += 1
                    else:
                        event_url = f'https://results.advancedeventsystems.com/api/event/{event_key}'
                        event_urls.append(event_url)
                        added_count += 1
                
                print(f"Skipped {skipped_count} existing events, added {added_count} new events")

            else:
                print(f"Failed to fetch data: HTTP {response.status_code}")
                   
        return event_urls
    except Exception as e:
        print(f"Error trying to get event list: {e}")
        return event_urls


# get a list of division urls from an event
def getEventData(urls):
    batch_size = 10  # Process in batches
    processed = 0
    
    for url in urls:
        try:
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

                eventId = data["EventId"]
                eventName = data["Name"]
                location = data["Location"]
                startDate = data["StartDate"]

                # Insert event data
                cursor.execute('''
                INSERT OR REPLACE INTO events (eventId, eventName, location, startDate)
                VALUES (?, ?, ?, ?)
                ''', (eventId, eventName, location, startDate))

                # Batch process divisions
                division_batch = []
                for division in data["Divisions"]:
                    divisionId = division["DivisionId"]
                    divisionName = division["Name"]
                    teamCount = division["TeamCount"]
                    codeAlias = division["CodeAlias"]
                    division_url = f'https://results.advancedeventsystems.com/odata/{eventKey}/standings(dId={divisionId},cId=null,tIds=[])?$orderby=OverallRank,FinishRank,TeamName,TeamCode'

                    division_batch.append((divisionId, eventId, divisionName, teamCount, codeAlias, division_url))
                    division_urls.append(division_url)

                # Batch insert divisions
                cursor.executemany('''
                INSERT OR REPLACE INTO divisions (divisionId, eventId, divisionName, teamCount, codeAlias, division_url)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', division_batch)

                print(f"Added {len(division_batch)} divisions for event {eventKey}")

                processed += 1
                
                # Commit every batch_size events to reduce I/O
                if processed % batch_size == 0:
                    conn.commit()
                    print(f"Committed batch of {batch_size} events")
                    gc.collect()  # Force garbage collection
                
                time.sleep(0.5)  # Reduced sleep time

            else:
                print(f"Failed to fetch {eventKey}: HTTP {response.status_code}")

        except Exception as e:
            print(f"Error processing event {url}: {e}")
            continue
    
    # Final commit for remaining events
    conn.commit()
    print(f"Completed processing {processed} events")


def getDivisionsForTourney(urls):
    print("getting division data..")
    batch_size = 50  # Process teams in batches
    processed_urls = 0

    for i, url in enumerate(urls):
        try:
            parts = url.split('/')
            eventJibberish = parts[4]

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36", 
                "Accept": "application/json, text/plain, */*",
            }

            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()

                # Batch process teams
                team_batch = []
                enrollment_batch = []
                
                for team in data["value"]:
                    teamId = team["TeamId"]
                    teamName = team["TeamName"]
                    teamCode = team["TeamCode"]
                    
                    if "Club" in team and team["Club"] is not None:
                        clubId = team["Club"]["ClubId"]
                        clubName = team["Club"]["Name"]
                    else:
                        clubId = None
                        clubName = None
                    
                    divisionId = team["Division"]["DivisionId"]
                    matchesWon = team["MatchesWon"]
                    matchesLost = team["MatchesLost"]
                    setsWon = team["SetsWon"]
                    setsLost = team["SetsLost"]
                    finishRank = team["FinishRank"]
                    overallRank = team["OverallRank"]
                    matchUrl = f'https://results.advancedeventsystems.com/api/event/{eventJibberish}/division/{divisionId}/team/{teamId}/schedule/past'

                    # Extract age from teamCode (2nd and 3rd characters)
                    teamAge = None
                    if teamCode and len(teamCode) >= 3:
                        try:
                            age_str = teamCode[1:3]  # Extract 2nd and 3rd characters
                            teamAge = int(age_str)
                        except ValueError:
                            teamAge = None

                    team_batch.append((teamId, teamName, teamCode, clubId, clubName, teamAge))
                    enrollment_batch.append((teamId, divisionId, matchesWon, matchesLost, setsWon, setsLost, finishRank, overallRank, matchUrl))
                    match_urls.append(matchUrl)

                # Batch insert teams and enrollments
                cursor.executemany('''
                INSERT OR REPLACE INTO teams (
                    teamId, teamName, teamCode, clubId, clubName, teamAge
                ) VALUES (?, ?, ?, ?, ?, ?)
                ''', team_batch)

                cursor.executemany('''
                INSERT OR REPLACE INTO enrollments (
                    teamId, divisionId, matchesWon, matchesLost,
                    setsWon, setsLost, finishRank, overallRank, matchUrl
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', enrollment_batch)

                print(f"Added {len(team_batch)} teams from division {i+1}/{len(urls)}")

                processed_urls += 1
                
                # Commit every batch_size divisions
                if processed_urls % batch_size == 0:
                    conn.commit()
                    print(f"Committed batch of {batch_size} divisions")
                    gc.collect()  # Force garbage collection
                
                time.sleep(0.3)  # Reduced sleep time

            else:
                print(f"Failed to fetch division {i+1}: HTTP {response.status_code}")

        except Exception as e:
            print(f"Error processing division {i+1}: {e}")
            continue
    
    # Final commit
    conn.commit()
    print(f"Completed processing {processed_urls} divisions")


# get a list of matches from a team
def getMatchData(urls):
    print(f'getting match data for {len(urls)} matches..')
    batch_size = 100  # Process matches in batches
    processed_urls = 0
    
    for i, url in enumerate(urls):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36", 
                "Accept": "application/json, text/plain, */*",
            }

            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                
                # Batch process matches
                match_batch = []
                
                for match in data:
                    bracket = match["Play"]["CompleteShortName"]
                    first_team = match["Match"]["FirstTeamName"]
                    first_team_id = match["Match"]["FirstTeamId"]
                    second_team = match["Match"]["SecondTeamName"]
                    second_team_id = match["Match"]["SecondTeamId"]
                    second_team_won = match["Match"]["SecondTeamWon"]
                    match_datetime = match["Match"]["ScheduledStartDateTime"]
                    
                    # Initialize set scores
                    set1_team1 = set1_team2 = set2_team1 = set2_team2 = set3_team1 = set3_team2 = None
                    
                    # Extract set scores
                    for j, set_score in enumerate(match["Match"]["Sets"], 1):
                        if j == 1:
                            set1_team1 = set_score["FirstTeamScore"]
                            set1_team2 = set_score["SecondTeamScore"]
                        elif j == 2:
                            set2_team1 = set_score["FirstTeamScore"]
                            set2_team2 = set_score["SecondTeamScore"]
                        elif j == 3:
                            set3_team1 = set_score["FirstTeamScore"]
                            set3_team2 = set_score["SecondTeamScore"]

                    # Parse datetime string to datetime object
                    parsed_datetime = None
                    if match_datetime:
                        try:
                            parsed_datetime = datetime.fromisoformat(match_datetime)
                        except Exception:
                            parsed_datetime = None
                    
                    match_batch.append((
                        bracket, first_team_id, second_team_id, second_team_won,
                        set1_team1, set1_team2, set2_team1, set2_team2,
                        set3_team1, set3_team2, parsed_datetime
                    ))

                # Batch insert matches
                if match_batch:
                    cursor.executemany('''
                    INSERT INTO matches (
                        bracket, team1_id, team2_id, team2_won,
                        set1_team1_score, set1_team2_score,
                        set2_team1_score, set2_team2_score,
                        set3_team1_score, set3_team2_score,
                        match_datetime
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', match_batch)

                print(f"Added {len(match_batch)} matches from team {i+1}/{len(urls)}")

                processed_urls += 1
                
                # Commit every batch_size URLs
                if processed_urls % batch_size == 0:
                    conn.commit()
                    print(f"Committed batch of {batch_size} team URLs")
                    gc.collect()  # Force garbage collection
                
                time.sleep(0.2)  # Reduced sleep time

            else:
                print(f"Failed to fetch matches for team {i+1}: HTTP {response.status_code}")

        except Exception as e:
            print(f"Error processing team {i+1}: {e}")
            continue
    
    # Final commit
    conn.commit()
    print(f"Completed processing {processed_urls} team URLs")



def remove_duplicate_matches():
    """Remove duplicate matches where team1_id and team2_id are swapped"""
    print("Removing duplicate matches...")

    # Get total count before cleanup
    cursor.execute("SELECT COUNT(*) FROM matches")
    total_matches = cursor.fetchone()[0]
    print(f"Total matches before cleanup: {total_matches}")
    
    # Use a more efficient query with EXISTS instead of JOIN
    cursor.execute('''
    DELETE FROM matches 
    WHERE matchId IN (
        SELECT m1.matchId
        FROM matches m1
        WHERE EXISTS (
            SELECT 1 FROM matches m2 
            WHERE (
                (m1.team1_id = m2.team2_id AND m1.team2_id = m2.team1_id)
                OR (m1.team1_id = m2.team1_id AND m1.team2_id = m2.team2_id)
            )
            AND m1.bracket = m2.bracket
            AND m1.match_datetime = m2.match_datetime
            AND m1.matchId > m2.matchId
        )
    )
    ''')
    
    deleted_count = cursor.rowcount
    conn.commit()
    
    print(f"Removed {deleted_count} duplicate matches")
    
    # Get total count after cleanup
    cursor.execute("SELECT COUNT(*) FROM matches")
    total_matches = cursor.fetchone()[0]
    print(f"Total matches remaining: {total_matches}")
    
    # Optimize database after cleanup
    cursor.execute("VACUUM")
    print("Database optimized")


# Close the database connection when done
try:
    initEventUrls()
    event_urls = getEventKeys()
    getEventData(event_urls)
    getDivisionsForTourney(division_urls)
    getMatchData(match_urls)
    
    # Remove duplicate matches
    remove_duplicate_matches()
finally:
    conn.close()

