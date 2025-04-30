import requests
import csv
import time



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

        filename = "eventDivisions.csv"
        with open(filename, "w", newline='', encoding="utf-8") as csvfile:
            fieldnames = ["field1", "field2", "field3"]

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

            for division in data["Divisions"]:

                divisionId = division["DivisionId"]
                divisionName = division["Name"]
                teamCount = division["TeamCount"]
                codeAlias = division["CodeAlias"]
                division_url = f'https://results.advancedeventsystems.com/odata/{eventKey}/standings(dId={divisionId},cId=null,tIds=[])?$orderby=OverallRank,FinishRank,TeamName,TeamCode'

                # create data row
                row = {
                    "eventId" : eventId,
                    "eventName" : eventName,
                    "location" : location,
                    "divisionId" : divisionId,
                    "divisionName" : divisionName,
                    "teamCount" : teamCount,
                    "codeAlias" : codeAlias,
                    "division_url" : division_url
                }

                print(row)

                division_urls.append(division_url)

                # Append to the CSV
                with open(filename, "a", newline='', encoding="utf-8") as csvfile:
                    fieldnames = row.keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writerow(row)

            time.sleep(1)

        else:
            print("Failed:", response.status_code)


def getDivisionsForTourney(urls):
    print("getting division data..")
    filename = "teams.csv"
    with open(filename, "w", newline='', encoding="utf-8") as csvfile:
        fieldnames = ["field1", "field2", "field3"]
        # writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        # writer.writerow(row)

    # urls = ["https://results.advancedeventsystems.com/odata/PTAwMDAwMzY3OTY90/standings(dId=169812,cId=null,tIds=[])?$orderby=OverallRank,FinishRank,TeamName,TeamCode"]

    for i, url in enumerate(urls):

        # Split the URL by '/'
        parts = url.split('/')
        # Get the part between the 4th and 5th slashes (index 4)
        eventJibberish = parts[4]

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36", 
            "Accept": "application/json, text/plain, */*",
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()  # for JSON responses
            # print(data)

            for team in data["value"]:

                teamId = team["TeamId"]
                teamName = team["TeamName"]
                teamCode = team["TeamCode"]
                matchesWon = team["MatchesWon"]
                matchesLost = team["MatchesLost"]
                setsWon = team["SetsWon"]
                setsLost = team["SetsLost"]
                finishRank = team["FinishRank"]
                overallRank = team["OverallRank"]
                divisionId = team["Division"]["DivisionId"]
                divionName = team["Division"]["Name"]
                clubId = team["Club"]["ClubId"]
                clubname = team["Club"]["Name"]

                # create data row
                row = {
                    "teamId" : teamId,
                    "teamName" : teamName,
                    "teamCode" : teamCode,
                    "matchesWon" : matchesWon,
                    "matchesLost" : matchesLost,
                    "setsWon" : setsWon,
                    "setsLost" : setsLost,
                    "finishRank" : finishRank,
                    "overallRank" : overallRank, 
                    "divisionId" : divisionId,
                    "divionName" : divionName,
                    "clubId" : clubId,
                    "clubname" : clubname,
                    "matchUrl" : f'https://results.advancedeventsystems.com/api/event/{eventJibberish}/division/{divisionId}/team/{teamId}/schedule/past'
                }

                print(row)

                match_urls.append(f'https://results.advancedeventsystems.com/api/event/{eventJibberish}/division/{divisionId}/team/{teamId}/schedule/past')

                # Append to the CSV
                with open(filename, "a", newline='', encoding="utf-8") as csvfile:
                    fieldnames = row.keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writerow(row)
            time.sleep(1)

        else:
            print("Failed:", response.status_code)


# get a list of matches from a team
def getMatchData(urls):
    print(f'getting match data for {len(urls)} matches..')
    
    filename = "matches.csv"
    with open(filename, "w", newline='', encoding="utf-8") as csvfile:
        fieldnames = ["field1", "field2", "field3"]
        # writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        # writer.writerow(row)

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
                second_team = match["Match"]["SecondTeamName"]
                original_sets = match["Match"]["Sets"]

                # Create a simplified version with only the two score fields
                simplified_sets = [
                    {
                        "FirstTeamScore": s["FirstTeamScore"],
                        "SecondTeamScore": s["SecondTeamScore"]
                    }
                    for s in original_sets
                ]
                
                # score_texts = [set_obj["ScoreText"] for set_obj in match["Match"]["Sets"]]
                second_team_won = match["Match"]["SecondTeamWon"]

                # create data row
                row = {
                    "CompleteShortName": bracket,
                    "Team1" : first_team,
                    "Team2" : second_team,
                    "Team2 Won?": second_team_won
                }

                for i, score in enumerate(simplified_sets, start=1):
                    row[f"Set{i}"] = score

                print(row)

                # Append to the CSV
                with open(filename, "a", newline='', encoding="utf-8") as csvfile:
                    fieldnames = row.keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writerow(row)
            time.sleep(1)

        else:
            print("Failed:", response.status_code)


getEventData(event_url)

getDivisionsForTourney(division_urls)

getMatchData(match_urls)
