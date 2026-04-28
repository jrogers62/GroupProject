import sqlite3
import csv
import os

# ====================== CONFIG ======================
DB_NAME = "NHL_DATA_V1.6.db"

# Put your CSV filenames here (change if your filenames are different)
CSV_FILES = {
    "Players":      "players.csv",
    "Teams":        "Teams.csv",
    "Games":        "Games.csv",
    "Skater_Stats": "Skater_Stats.csv",
    "Goalie_Stats": "Goalie_Stats.csv",
    "Plays":        "Plays.csv",
    "Play_Involvement": "Play_Involvement.csv",
    "Lines":        "Lines.csv",          # adjust if needed
    "Referee":      "Referee.csv",
    "Team_Stats":   "Team_Stats.csv",
    # Add more CSVs here if you have them
}

# ===================================================

def main():
    if os.path.exists(DB_NAME):
        print(f"Deleting old database: {DB_NAME}")
        os.remove(DB_NAME)

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    print("Creating tables and importing data...")

    # ====================== CREATE TABLES ======================
    cur.executescript("""
        DROP TABLE IF EXISTS Players;
        DROP TABLE IF EXISTS Teams;
        DROP TABLE IF EXISTS Games;
        DROP TABLE IF EXISTS Skater_Stats;
        DROP TABLE IF EXISTS Goalie_Stats;
        DROP TABLE IF EXISTS Plays;
        DROP TABLE IF EXISTS Play_Involvement;
        DROP TABLE IF EXISTS Lines;
        DROP TABLE IF EXISTS Referee;
        DROP TABLE IF EXISTS Team_Stats;

        CREATE TABLE Players (
            playerID INTEGER PRIMARY KEY,
            first TEXT,
            last TEXT,
            birthCountry TEXT,
            birthCity TEXT,
            birthStateProvince TEXT,
            position TEXT,
            heightCM REAL,
            birthDate TEXT,
            weight INTEGER
        );

        CREATE TABLE Teams (
            teamID INTEGER PRIMARY KEY,
            shortName TEXT,
            teamName TEXT,
            abbreviation TEXT
        );

        CREATE TABLE Games (
            gameID INTEGER PRIMARY KEY,
            homeGoals INTEGER,
            awayGoals INTEGER,
            dateTimeGMT TEXT,
            type TEXT
        );

        CREATE TABLE "Skater_Stats" (
            playerID INTEGER,
            gameID INTEGER,
            IFXOnGoal REAL,
            shots INTEGER,
            faceOffWins INTEGER,
            faceOffLosses INTEGER,
            points INTEGER,
            rebounds INTEGER,
            PIM INTEGER,
            assists INTEGER,
            timeOnIce INTEGER,
            shortHandedGoals INTEGER,
            powerPlayGoals INTEGER,
            evenGoals INTEGER,
            plusMinus INTEGER,
            takeaways INTEGER,
            blocked INTEGER,
            hits INTEGER,
            PRIMARY KEY (playerID, gameID)
            FOREIGN KEY (playerID) REFERENCES "Players"
            FOREIGN KEY (gameID) REFERENCES "Games"
        );

        CREATE TABLE "Goalie_Stats" (
            playerID INTEGER,
            gameID INTEGER,
            goals INTEGER,
            savePercentage REAL,
            freeze INTEGER,
            shots INTEGER,
            saves INTEGER,
            outcome TEXT,
            PRIMARY KEY (playerID, gameID)
            FOREIGN KEY (playerID) REFERENCES "Players"
            FOREIGN KEY (gameID) REFERENCES "Games"
        );

        CREATE TABLE "Plays" (
            playID INTEGER PRIMARY KEY AUTOINCREMENT,
            originalPlayID TEXT,
            gameID INTEGER,
            period INTEGER,
            periodTime INTEGER,
            type TEXT,
            secondaryType TEXT,
            FOREIGN KEY (gameID) REFERENCES "Games"
        );

        CREATE TABLE "Play_Involvement" (
            playID INTEGER,
            playerID INTEGER,
            gameID INTEGER,
            role TEXT,
            PRIMARY KEY (playID, playerID),
            FOREIGN KEY (playID) REFERENCES "Plays",
            FOREIGN KEY (gameID) REFERENCES "Games",
            FOREIGN KEY (playerID) REFERENCES "Players"
        );

        CREATE TABLE "Lines" (
            lineID INTEGER PRIMARY KEY AUTOINCREMENT,
            originalLineID REAL,
            name TEXT,
            teamID INTEGER,
            iceTime INTEGER,
            xOnGoal INTEGER,
            shots INTEGER,
            goals INTEGER,
            rebounds INTEGER,
            penalties INTEGER,
            hits INTEGER,
            FOREIGN KEY (teamID) REFERENCES "Teams"
        );

        CREATE TABLE "Referee" (
            RID INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            type TEXT
        );

        CREATE TABLE "Team_Stats" (
            gameID INTEGER,
            teamID INTEGER,
            won TEXT,
            hits INTEGER,
            goals INTEGER,
            shots INTEGER,
            PIM INTEGER,
            homeOrAway INTEGER,
            powerPlayGoals INTEGER,
            takeaways INTEGER,
            giveaways INTEGER,
            blockedShots INTEGER,
            faceOffWinPercentage INTEGER,
            FOREIGN KEY (gameID) REFERENCES "Games",
            FOREIGN KEY (teamID) REFERENCES "Teams",
            PRIMARY KEY (teamID, gameID)
        );
    """)

    # ====================== IMPORT CSVs ======================
    for table, csv_file in CSV_FILES.items():
        if not os.path.exists(csv_file):
            print(f"Warning: {csv_file} not found, skipping {table}")
            continue

        print(f"Importing {csv_file} into {table}...")
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)  # skip header row

            # Create placeholders
            placeholders = ','.join(['?'] * len(headers))
            sql = f"INSERT INTO {table} VALUES ({placeholders})"

            for row in reader:
                cur.execute(sql, row)

    conn.commit()
    conn.close()
    print(f"\n✅ Database successfully populated: {DB_NAME}")

if __name__ == "__main__":
    main()