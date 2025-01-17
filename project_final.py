import mysql.connector as mysql
from mysql.connector import Error
import csv
import pandas as pd
import numpy as np
from tabulate import tabulate
import re
from colorama import Fore, Style
import time
import sys

def loading_animation(text="Loading"):
    """
    Displays a loading animation with the given text.
    """
    for i in range(10):  # Number of animation cycles
        sys.stdout.write(f"\r{text} {'.' * (i % 4)}")  # Cycle through dots
        sys.stdout.flush()
        time.sleep(0.3)  # Delay between updates
    print("\r" + " " * len(text + " ...") + "\r", end="")


##________________________________________________________________________
# STEP 1: CLEANING OUR DATABASE FOR PROPER QUERY EXECUTION

##__________________________________________________
# 1.1 Filling missing values for Shazam Charts and Key

df = pd.read_csv('Spotify.csv')

# Print missing value counts before cleaning
missing_counts = df.isnull().sum()
print("Missing Value Counts BEFORE CLEANING:")
print(missing_counts)

# Fill missing values in the `in_spotify_playlist` column with 0
if 'in_shazam_charts' in df.columns:
    df['in_shazam_charts'].fillna(0, inplace=True)

# Fill missing values in the `key` column with a random letter from A to G
random_letters = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
if 'key' in df.columns:
    df['key'] = df['key'].apply(
        lambda x: np.random.choice(random_letters) if pd.isnull(x) else x
    )
# Save the cleaned dataset
df.to_csv('Spotify.csv', index=False)

# Print missing value counts after cleaning to ensure completion
missing_counts = df.isnull().sum()
print("Missing Value Counts AFTER CLEANING:")
print(missing_counts)

##__________________________________________________
# 1.2 Removing faulty row that had concatenated string within "streams" column

track_name_to_remove = "Love Grows (Where My Rosemary Goes)"
artist_name_to_remove = "Edison Lighthouse"
df = df[~((df['track_name'] == track_name_to_remove) & (df['artist(s)_name'] == artist_name_to_remove))]

# Save the modified DataFrame back to the same CSV file
df.to_csv('Spotify.csv', index=False)

print(f"The song '{track_name_to_remove}' by '{artist_name_to_remove}' has been removed.")

##__________________________________________________
# 1.3 Replacing non-conventional characters with letters for human-readability

def replace_characters(value):
    if isinstance(value, str):  # Only process string values
        # Regex to match characters that are not letters, digits, spaces, or allowed punctuation
        return re.sub(
            r'[^a-zA-Z0-9\s.,!?:;()\'\"\-]',  # Matches characters NOT in the allowed set
            lambda _: np.random.choice(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")),  # Replace with random letter
            value
        )
    return value

# Apply the function to the 'track_name' and 'artist(s)_name' columns
df['track_name'] = df['track_name'].apply(replace_characters)
df['artist(s)_name'] = df['artist(s)_name'].apply(replace_characters)

# Save the cleaned DataFrame back to the CSV
df.to_csv('Spotify.csv', index=False)

print("Unconventional characters in 'track_name' and 'artist(s)_name' have been replaced with random letters.")

##__________________________________________________ 
# 1.4 Uses process of normalization to assess and drop duplicates (that are lower performing)

# Load the dataset
file_path = 'Spotify.csv'
df = pd.read_csv(file_path)

# Function to normalize strings
def normalize_string(value):
    if isinstance(value, str):
        value = value.lower()  # Convert to lowercase
        value = re.sub(r'[^a-z0-9\s]', '', value)  # Remove special characters
        value = re.sub(r'\s+', ' ', value).strip()  # Remove extra spaces
    return value

# Normalize the track and artist names
df['normalized_track_name'] = df['track_name'].apply(normalize_string)
df['normalized_artist_name'] = df['artist(s)_name'].apply(normalize_string)

# Sort by streams in descending order (higher streams first)
df = df.sort_values('streams', ascending=False)

# Drop duplicates based on normalized names, keeping the row with the highest streams
df = df.drop_duplicates(subset=['normalized_track_name', 'normalized_artist_name'], keep='first')

# Drop the temporary normalized columns
df = df.drop(columns=['normalized_track_name', 'normalized_artist_name'])

# Save the cleaned DataFrame back to the original CSV file
df.to_csv(file_path, index=False)

print(f"Duplicates removed and cleaned dataset saved back to the original file: {file_path}")




##________________________________________________________________________
# STEP 2: CREATING DATABASE 

df.to_csv('Spotify.csv', index=False)

def createdb(user: str, passw: str):
    db = None
    try:
        db = mysql.connect(host="localhost", user=user, passwd=passw)
        curs = db.cursor()
        curs.execute("CREATE DATABASE IF NOT EXISTS Spotify")
        print("Database created or already exists")
    except Error as e:
        print("Error:", e)
    finally:
        if db is not None and db.is_connected():
            curs.close()
            db.close()
            print("MySQL connection is closed")


##________________________________________________________________________
# STEP 3: CREATING TABLES FROM ER DIAGRAM SETUP 

def creattables(user: str, passw: str):
    db = None
    try:
        db = mysql.connect(host="localhost", user=user, passwd=passw, database="Spotify")
        curs = db.cursor()

        artist_table = """
        CREATE TABLE Artist(
            Artist_Name VARCHAR(500) PRIMARY KEY
        );
        """

        track_table = """
        CREATE TABLE Track(
            Track_ID INT AUTO_INCREMENT PRIMARY KEY,
            Track_Name VARCHAR(500) NOT NULL,
            Cover_URL VARCHAR(255) NOT NULL,
            Artist_Count SMALLINT NOT NULL
        );
        """

        released_by_table = """
        CREATE TABLE Released_By(
            Track_ID INT,
            Artist_Name VARCHAR(500),
            PRIMARY KEY (Track_ID, Artist_Name),
            FOREIGN KEY (Track_ID) REFERENCES Track(Track_ID) ON DELETE CASCADE,
            FOREIGN KEY (Artist_Name) REFERENCES Artist(Artist_Name) ON DELETE CASCADE
        );
        """

        info_table = """ 
        CREATE TABLE StreamingInfo(
            Info_ID INT AUTO_INCREMENT PRIMARY KEY,
            Apple_Playlists INT NOT NULL,
            Apple_Charts INT NOT NULL,
            Spotify_Charts INT NOT NULL,
            Spotify_Playlists INT NOT NULL,
            Streams BIGINT NOT NULL, 
            Deezer_Charts INT NOT NULL,
            Deezer_Playlists INT NOT NULL,
            Shazam_Charts INT NOT NULL
        );
        """

        profile_table = """
        CREATE TABLE TrackProfile(
            Profile_ID INT AUTO_INCREMENT PRIMARY KEY,
            Liveliness SMALLINT NOT NULL,
            Instrumentalness SMALLINT NOT NULL,
            Mode VARCHAR(10) NOT NULL,
            Music_Key VARCHAR(10) NOT NULL,
            Bpm SMALLINT NOT NULL,
            Speechiness SMALLINT NOT NULL,
            Acoustiness SMALLINT NOT NULL,
            Valence SMALLINT NOT NULL,
            Danceability SMALLINT NOT NULL,
            Energy SMALLINT NOT NULL,
            Released_Day SMALLINT NOT NULL,
            Released_Month SMALLINT NOT NULL,
            Released_Year SMALLINT NOT NULL
        );
        """
        curs.execute(artist_table)
        curs.execute(track_table)
        curs.execute(info_table)
        curs.execute(profile_table)
        curs.execute(released_by_table)

        print("Tables created successfully.")
    except Error as e:
        print("Error", e)
    finally:
        if db.is_connected():
            curs.close()
            db.close()
            print("MySQL connection is closed")



##________________________________________________________________________
# STEP 4: LOADING DATA INTO TABLES

def dataload(user: str, passwd: str, csv_file: str):
    """
    Load data from a CSV file into the database.
    """
    try:
        db = mysql.connect(
            host="localhost",
            user=user,
            passwd=passwd,
            database="Spotify"
        )
        cursor = db.cursor()

        with open(csv_file, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)

            for row in csv_reader:

                track_insert_query = """
                INSERT IGNORE INTO Track (Track_Name, Cover_URL, Artist_Count)
                VALUES (%s, %s, %s)
                """

                cursor.execute(track_insert_query, (
                    row['track_name'],
                    row['cover_url'],
                    row['artist_count']
                ))

                track_id = cursor.lastrowid

                artist_names = [artist.strip() for artist in row['artist(s)_name'].split(',')]

                for artist_name in artist_names:
                    artist_insert_query = """
                    INSERT IGNORE INTO Artist (Artist_Name)
                    VALUES (%s)
                    """
                    cursor.execute(artist_insert_query, (artist_name,))
                    
                    released_by_insert_query = """
                    INSERT IGNORE INTO Released_By (Track_ID, Artist_Name)
                    VALUES (%s, %s)
                    """
                    cursor.execute(released_by_insert_query, (track_id, artist_name))
            

                info_insert_query = """
                INSERT IGNORE INTO StreamingInfo (Apple_Playlists, Apple_Charts, Spotify_Charts, Spotify_Playlists, Streams, Deezer_Charts, Deezer_Playlists, Shazam_Charts)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """

                cursor.execute(info_insert_query, (
                    row['in_apple_playlists'],
                    row['in_apple_charts'],
                    row['in_spotify_charts'],
                    row['in_spotify_playlists'],
                    row['streams'],
                    row['in_deezer_charts'],
                    int(row['in_deezer_playlists'].replace("," , "")),  
                    row['in_shazam_charts']
                ))

                profile_insert_query = """
                INSERT IGNORE INTO TrackProfile (
                    Liveliness, Instrumentalness, Mode, Music_Key, Bpm, Speechiness, Acoustiness, Valence, Danceability,
                    Energy, Released_Day, Released_Month, Released_Year
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
   
                cursor.execute(profile_insert_query, (
                    row['liveness_%'],
                    row['instrumentalness_%'],
                    row['mode'],
                    row['key'],
                    row['bpm'],
                    row['speechiness_%'],
                    row['acousticness_%'],
                    row['valence_%'],
                    row['danceability_%'],
                    row['energy_%'],
                    row['released_day'],
                    row['released_month'],
                    row['released_year']
                ))

        db.commit()
        print("Data loaded successfully into the database.")

    except Error as e:
        print("Error:", e)

    finally:
        if db.is_connected():
            cursor.close()
            db.close()
            print("MySQL connection is closed.")

user = "root"
password = ""

try:
    print("creating the database...")

    loading_animation("Setting up the database") 
    createdb(user=user, passw=password)
    print("defining tables...")

    loading_animation("Creating tables")  
    creattables(user=user, passw=password)
    print("loading the dataset into the DB...")

    loading_animation("Loading data into database")  
    dataload(user=user, passwd=password, csv_file='Spotify.csv')
except Error as e:
    print(f"The database already exists, running queries only...")

def loading_animation(text="Loading"):
    """
    Displays a loading animation with the given text.
    """
    for i in range(10):  
        sys.stdout.write(f"\r{text} {'.' * (i % 4)}")  
        sys.stdout.flush()
        time.sleep(0.3)  
    print("\r" + " " * len(text + " ...") + "\r", end="")        

##________________________________________________________________________
# STEP 4: QUERY EXPOSITION AND EXECUTING THEM

def execute_query(query):
    """
    Executes the given SQL query and displays the result.
    """
    loading_animation("Executing query")  
    try:
        connection = mysql.connect(
            host="localhost",
            user="root",
            password="",
            database="Spotify"
        )
        cursor = connection.cursor(buffered=True)

        for result in cursor.execute(query, multi=True):
            if result.with_rows: 
                columns = [desc[0] for desc in result.description]  
                rows = result.fetchall()  
                if rows:
                    print(tabulate(rows, headers=columns, tablefmt="fancy_grid"))
                else:
                    print(Fore.RED + "Query executed successfully but returned no results." + Style.RESET_ALL)

        cursor.close()
        connection.close()

    except mysql.Error as e:
        print(Fore.RED + f"Error: {e}" + Style.RESET_ALL)


def display_menu(queries):
    """
    Display an interactive menu of queries with colors and formatting.
    """
    print(Fore.CYAN + Style.BRIGHT + "\nHELLO! Welcome to our Spotify Database!" + Style.RESET_ALL)
    print(Fore.YELLOW + "Here are the available queries:\n" + Style.RESET_ALL)

    menu_data = [
        [key, value["description"]] for key, value in queries.items()
    ]
    print(tabulate(menu_data, headers=["Option", "Query Description"], tablefmt="fancy_grid"))

    print(Fore.RED + "\n0. Exit" + Style.RESET_ALL)

    choice = input(Fore.GREEN + "\nEnter the number of the query you find interesting! (1-10): " + Style.RESET_ALL).strip()
    return choice


def main():
    queries = {
        1: {
            "description": "Cultural Trends: How Have They Evolved Over a Century?",
            "Qinfo": """
            -> Your wish is our command! This ouput provides insight into how track features have evolved across the span of a century. 
            With this information we are reflecting fluctuations in public taste and facilitating researchers or 
            music sommeliers in correlating possible cultural/socioeconomic events during these years to the 
            released music in chronological publication.""",
            "query": """
            SELECT 
                Released_Year,                           
                AVG(Bpm) AS avg_bpm,                     
                AVG(Danceability) AS avg_danceability,   
                COUNT(Profile_ID) AS total_songs         
            FROM TrackProfile                            
            WHERE Released_Year IS NOT NULL              
            GROUP BY Released_Year                       
            ORDER BY Released_Year;                      
            """
        },

        2 : {
            "description": "Top Charting Tracks in Shazam",
            "Qinfo": """
            -> Your wish is our command! This output gives insight about Shazam trends and helps identify
            songs that are popular among people discovering new music. This allows us to find niche trends:
            Shazam is a platform for new music discovery and if a song is interesting enough to compel a
            first-time listener, these tracks are gold-mines for music researchers.
            """,
            "query":"""         
            SELECT
                t.Track_Name AS track_name, 
                GROUP_CONCAT(DISTINCT rb.Artist_Name) AS artist_name, 
                si.Shazam_Charts AS in_shazam_charts, 
                MAX(si.Streams) AS streams 
            FROM
                Track t 
            JOIN
                Released_By rb ON t.Track_ID = rb.Track_ID 
            JOIN
                StreamingInfo si ON t.Track_ID = si.Info_ID 
            WHERE
                si.Shazam_Charts IS NOT NULL 
            GROUP BY
                t.Track_Name, si.Shazam_Charts 
            ORDER BY
                si.Shazam_Charts DESC 
            LIMIT 10; 
            """
        },
       
        3 : {
            "description": "Tracks Most Present Accross ALL Platforms",
            "Qinfo":"""
            -> Your wish is our command! This ouput allows us to identify the Top 10 Tracks 
            with the most presence in playlist platforms available to our database; this 
            includes Spotify, Apple Music, and Deezer. This may help you identify the songs
            with the greatest appeal across the most used platforms!""",
            "query": """
                SELECT 
                    t.Track_Name AS track_name,
                    GROUP_CONCAT(DISTINCT rb.Artist_Name) AS artist_name, 
                    SUM(DISTINCT si.Spotify_Playlists + si.Apple_Playlists + si.Deezer_Playlists) AS total_playlist_presence
                FROM 
                    Track t
                JOIN 
                    Released_By rb ON t.Track_ID = rb.Track_ID
                JOIN 
                    StreamingInfo si ON t.Track_ID = si.Info_ID
                GROUP BY 
                    t.Track_Name
                ORDER BY 
                    total_playlist_presence DESC
                LIMIT 10;
            """
        },

        4: {
            "description": "Could BPM Range Correlate with Stream Success?",
            "Qinfo": """ 
            -> Your wish is our command! This output allows us to identify the Top 10 Tracks 
            with the highest streams and their BPM range. By analyzing this data, we can gain 
            insights into the tempo characteristics that are most associated with popular songs. 
            Whether you're exploring music trends or crafting playlists, this data can guide 
            your understanding of how BPM influences audience appeal!
            """,
            "query": """
                SELECT 
                    t.Track_Name AS track_name,
                    GROUP_CONCAT(DISTINCT rb.Artist_Name) AS artist_name,
                    si.Streams AS streams,
                    tp.Bpm AS bpm
                FROM 
                    Track t
                JOIN 
                    Released_By rb ON t.Track_ID = rb.Track_ID
                JOIN 
                    StreamingInfo si ON t.Track_ID = si.Info_ID
                JOIN 
                    TrackProfile tp ON t.Track_ID = tp.Profile_ID
                WHERE 
                    si.Streams IS NOT NULL
                GROUP BY 
                    t.Track_Name, tp.Bpm, si.Streams

                ORDER BY 
                    si.Streams DESC
                LIMIT 10;
            """
        },
        
        5 : {
            "description": "Liveliness and Album Cover Correlation",
            "Qinfo": """
            -> Your wish is our command! This ouput, establishing a liveliness threshold above 
            70percent liveliness, allows us to find a possible correlation between an Album Cover 
            presence and the track's ability to be engaging/lively. This is highly interpretable 
            and is valuable to see how much high-energy songs are visually represented by artists.
            """,
            "query": """
            WITH ValidCoverCount AS (
                SELECT 
                    COUNT(*) AS total_high_liveness_songs, 
                    SUM(
                        CASE 
                            WHEN t.Cover_URL IS NOT NULL 
                                AND t.Cover_URL NOT IN ('', 'Not Found') 
                            THEN 1 
                            ELSE 0 
                        END
                    ) AS songs_with_cover_url 
                FROM 
                    Track t 
                JOIN 
                    TrackProfile tp ON tp.Profile_ID = t.Track_ID 
                WHERE 
                    tp.Liveliness > 70 
            )
            SELECT 
                total_high_liveness_songs, 
                songs_with_cover_url, 
                ROUND(songs_with_cover_url * 100.0 / total_high_liveness_songs, 2) AS percentage_with_cover_url 
            FROM 
                ValidCoverCount; 
            """
        },

        6: {
            "description": "Top 5 Most Successful Artists, According to Spotify playlists",
            "Qinfo": """
            -> Your wish is our command! This ouput allows us to identify the Top 5 Artists 
            with the highest number of distinct tracks in Spotify Playlists. Here, we can measure a certain
            metric of popularity, given that Spotify has 626 million monthy active users!
            """,
            "query": """
            SELECT 
                rb.Artist_Name,  
                COUNT(DISTINCT t.Track_ID) AS tracks_in_playlists 
            FROM 
                Released_By rb 
            JOIN 
                Track t ON rb.Track_ID = t.Track_ID
            JOIN 
                StreamingInfo si ON t.Track_ID = si.Info_ID 
            WHERE 
                si.Spotify_Playlists > 0 
            GROUP BY 
                rb.Artist_Name 
            ORDER BY 
                tracks_in_playlists DESC
            LIMIT 5; 
            """
        },
  
        7: {
            "description": "Does Energy Category Predict Overall Streaming Patterns?",
            "Qinfo": """ 
            -> Your wish is our command! This output allows us to analyze 
            the average number of high streams by energy category. This can provide 
            insights into listener preferences and trends based on the energy levels of 
            tracks, enabling data-driven decisions in playlist curation and marketing strategies!
            """,
            "query": """
            WITH AvgStreams AS (
                SELECT AVG(Streams) AS avg_streams
                FROM StreamingInfo
            )
            SELECT
                Energy_Category,
                AVG(High_Streams) AS avg_high_streams
            FROM (
                SELECT 
                    CASE 
                        WHEN Energy < 30 THEN 'Low Energy' 
                        WHEN Energy BETWEEN 30 AND 70 THEN 'Medium Energy' 
                        ELSE 'High Energy' 
                    END AS Energy_Category,
                    Streams AS High_Streams
                FROM TrackProfile
                JOIN StreamingInfo 
                    ON TrackProfile.Profile_ID = StreamingInfo.Info_ID
                JOIN AvgStreams 
                    ON Streams > avg_streams 
            ) AS Energy_Classification
            GROUP BY Energy_Category;
            """
        },

        8: {
            "description": "How Energy Levels Impact Platform Presence",
            "Qinfo": """ 
            -> Your wish is our command! This query examines the relationship 
            between a track's energy category and its presence on different music 
            platforms. By categorizing platform presence into single-platform and 
            multi-platform exposure, and calculating average streams for each group, 
            this analysis reveals how energy levels influence a track's distribution 
            and success across platforms.
            """,
            "query": """
            -- Step 1: Categorize tracks based on their energy levels and collect related data
            WITH Energy_Categorization AS (
                SELECT DISTINCT 
                    t.Track_ID, 
                    t.Track_Name, 
                    rb.Artist_Name, 
                    tp.Energy, 
                    si.Streams, 
                    CASE
                        WHEN tp.Energy < 30 THEN 'Low Energy'
                        WHEN tp.Energy BETWEEN 30 AND 70 THEN 'Medium Energy' 
                        ELSE 'High Energy' 
                    END AS Energy_Category, 
                    si.Spotify_Charts, 
                    si.Deezer_Charts, 
                    si.Apple_Charts 
                FROM
                    TrackProfile tp 
                JOIN StreamingInfo si ON tp.Profile_ID = si.Info_ID 
                JOIN Track t ON t.Track_ID = tp.Profile_ID 
                JOIN Released_By rb ON rb.Track_ID = t.Track_ID 
            ),
            Platform_Presence_Categorization AS (
                SELECT
                    Track_ID, 
                    Track_Name, 
                    Artist_Name, 
                    Energy, 
                    Streams, 
                    Energy_Category, 
                    Spotify_Charts, 
                    Deezer_Charts, 
                    Apple_Charts, 
                    CASE
                        WHEN 
                            (Spotify_Charts = 0 AND Deezer_Charts = 0 AND Apple_Charts > 0) OR
                            (Spotify_Charts = 0 AND Deezer_Charts > 0 AND Apple_Charts = 0) OR
                            (Spotify_Charts > 0 AND Deezer_Charts = 0 AND Apple_Charts = 0)
                        THEN 'Single-Platform Presence'
                        WHEN 
                            (Spotify_Charts > 0 AND Deezer_Charts > 0 AND Apple_Charts > 0) OR
                            (Spotify_Charts > 0 AND Deezer_Charts > 0) OR
                            (Spotify_Charts > 0 AND Apple_Charts > 0) OR
                            (Deezer_Charts > 0 AND Apple_Charts > 0)
                        THEN 'Multi-Platform Presence' 
                        ELSE 'Other' 
                    END AS Platform_Presence 
                FROM
                    Energy_Categorization 
            )
            SELECT
                Energy_Category, 
                Platform_Presence, 
                ROUND(AVG(Streams), 2) AS avg_streams, 
                COUNT(DISTINCT Track_ID) AS unique_tracks 
            FROM (
                SELECT DISTINCT
                    Track_ID, 
                    Energy_Category, 
                    Platform_Presence, 
                    Streams 
                FROM
                    Platform_Presence_Categorization 
                WHERE
                    Platform_Presence IN ('Multi-Platform Presence', 'Single-Platform Presence')
            ) AS filtered_tracks 
            GROUP BY
                Platform_Presence, Energy_Category 
            ORDER BY
                FIELD(Energy_Category, 'High Energy', 'Medium Energy', 'Low Energy'), 
                Platform_Presence; 
            """
        },

        9: {
            "description": " Most Seasonal Tracks",
            "Qinfo": """ 
            -> Analyze the release patterns for tracks that are most frequently streamed during 
            specific months of the year. This can be extremely telling in terms of which months
            produce the most seasonally-streamed tracks. Utilizing these stats, one can produce 
            songs to have continuous relevance in designated months.
            """,
            "query": """
			SELECT 
                tp.Released_Month AS release_month, 
                COUNT(t.Track_ID) AS track_count, 
                ROUND(AVG(si.Streams), 2) AS avg_streams 
            FROM 
                Track t 
            JOIN 
                StreamingInfo si ON t.Track_ID = si.Info_ID 
            JOIN 
                TrackProfile tp ON t.Track_ID = tp.Profile_ID 
            GROUP BY 
                tp.Released_Month 
            ORDER BY 
                avg_streams DESC 
            LIMIT 12; 
            """,
        },
     
        10: {
            "description": "Tracks Dominating a Single Platform",
            "Qinfo": """ 
            -> Your wish is our command! This query identifies tracks that perform 
            best on a single platform—Spotify, Apple Music, or Deezer—by comparing 
            playlist presence. It highlights platform-specific dominance and 
            streaming trends for top tracks.
            """,
            "query": """
			SELECT 
                t.Track_Name, 
                GROUP_CONCAT(DISTINCT rb.Artist_Name) AS Artists, 
                MAX(si.Streams) AS Streams, 
                CASE
                    WHEN si.Spotify_Playlists > si.Apple_Playlists AND si.Spotify_Playlists > si.Deezer_Playlists THEN 'Spotify' 
                    WHEN si.Apple_Playlists > si.Spotify_Playlists AND si.Apple_Playlists > si.Deezer_Playlists THEN 'Apple Music' 
                    WHEN si.Deezer_Playlists > si.Spotify_Playlists AND si.Deezer_Playlists > si.Apple_Playlists THEN 'Deezer' 
                    ELSE 'Tied' 
                END AS Dominant_Platform 
            FROM Track t -- Select data from the `Track` table as the base table.
            JOIN Released_By rb ON t.Track_ID = rb.Track_ID 
            JOIN StreamingInfo si ON t.Track_ID = si.Info_ID 
            GROUP BY t.Track_Name, Dominant_Platform 
            ORDER BY Streams DESC 
            LIMIT 20; 
            """,
        }        
    }

##________________________________________________________________________
# STEP 5: INTERFACE CODE, INTERACTING WITH USER

    while True:
        choice = display_menu(queries)
        if choice == "0":
            print(Fore.RED + "Goodbye!" + Style.RESET_ALL)
            break  
        elif choice.isdigit() and int(choice) in queries:
            print(Fore.YELLOW + f"DEBUG: User selected query {choice}" + Style.RESET_ALL)

            query_description = queries[int(choice)]["description"]

            loading_animation("Fetching results")  
            print(Fore.BLUE + f"\nExecuting: {query_description}\n" + Style.RESET_ALL)

            try:
                execute_query(queries[int(choice)]["query"])
                if queries[int(choice)]['Qinfo']:
                    print(Fore.YELLOW + queries[int(choice)]['Qinfo'] + Style.RESET_ALL)
                else:
                    print(Fore.RED + "No additional information available for this query." + Style.RESET_ALL)
            except Exception as e:
                print(Fore.RED + f"Error executing query: {e}" + Style.RESET_ALL)

            another = input(Fore.GREEN + "\nWould you like to see another query? (y/n): " + Style.RESET_ALL).strip().lower()
            if another != 'y':
                print(Fore.RED + "Goodbye!" + Style.RESET_ALL)
                break  
        else:
            print(Fore.RED + "Invalid choice. Please try again!" + Style.RESET_ALL)


if __name__ == "__main__":
    main()
