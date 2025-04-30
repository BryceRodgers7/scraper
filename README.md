# Advanced Event Systems Scraper

A Python-based web scraper for extracting tournament data from Advanced Event Systems (AES) results pages.

## Features

- Extracts event data including divisions and teams
- Retrieves team standings and statistics
- Collects match results and scores
- Exports data to CSV files

## Output Files

The scraper generates three CSV files:
- `eventDivisions.csv`: Contains event and division information
- `teams.csv`: Contains team statistics and standings
- `matches.csv`: Contains match results and scores

## Requirements

- Python 3.x
- Required packages:
  - requests
  - csv
  - time

## Usage

1. Configure the `event_url` list in `samplescraper.py` with the desired tournament URLs
2. Run the script:
```bash
python samplescraper.py
```

## Data Structure

The scraper processes data in three main steps:
1. `getEventData()`: Extracts division information from events
2. `getDivisionsForTourney()`: Retrieves team data for each division
3. `getMatchData()`: Collects match results for each team

## Note

The script includes a 1-second delay between requests to avoid overwhelming the server. 