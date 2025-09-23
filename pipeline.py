import pandas as pd
import psycopg2
from textblob import TextBlob
import sys

# --- DATABASE CONNECTION DETAILS ---
# Replace with your actual PostgreSQL credentials
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "081808"  # IMPORTANT: Replace with the password you set during installation
DB_HOST = "localhost"
DB_PORT = "5444"

# --- FILE PATH ---
# Make sure your CSV file is in the same directory as this script,
# or provide the full path to it.
CSV_FILE_PATH = 'tweets.csv' # IMPORTANT: Change this to the name of your downloaded CSV file

def analyze_sentiment(text):
    """
    Analyzes the sentiment of a given text using TextBlob.
    Returns a tuple of (polarity, subjectivity).
    Polarity is a float between -1.0 (very negative) and 1.0 (very positive).
    Subjectivity is a float between 0.0 (very objective) and 1.0 (very subjective).
    """
    analysis = TextBlob(str(text))
    return analysis.sentiment.polarity, analysis.sentiment.subjectivity

def main():
    """
    Main function to run the ETL pipeline.
    """
    conn = None  # Initialize connection to None
    try:
        # --- CONNECT TO POSTGRESQL ---
        print("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()
        print("Connection successful!")

        # --- CREATE TABLE (if it doesn't exist) ---
        # This is an idempotent operation, safe to run multiple times.
        create_table_query = """
        CREATE TABLE IF NOT EXISTS tweets (
            id SERIAL PRIMARY KEY,
            tweet_text TEXT,
            polarity REAL,
            subjectivity REAL
        );
        """
        cur.execute(create_table_query)
        conn.commit()
        print("Table 'tweets' is ready.")

        # --- EXTRACT: READ DATA FROM CSV ---
        print(f"Reading data from {CSV_FILE_PATH}...")
        # IMPORTANT: You may need to adjust the column name 'text'
        # to match the actual column name of the tweet content in your CSV.
        df = pd.read_csv(CSV_FILE_PATH)
        # Let's check for a common column name like 'text' or 'tweet'
        tweet_column = None
        if 'text' in df.columns:
            tweet_column = 'text'
        elif 'tweet' in df.columns:
            tweet_column = 'tweet'
        else:
            print("Error: Could not find a 'text' or 'tweet' column in the CSV.")
            sys.exit() # Exit the script if no tweet column is found
            
        print("Data loaded successfully.")

        # --- TRANSFORM & LOAD ---
        print("Starting data processing and insertion...")
        insert_query = "INSERT INTO tweets (tweet_text, polarity, subjectivity) VALUES (%s, %s, %s);"

        for index, row in df.iterrows():
            tweet_text = row[tweet_column]
            
            # Transform: Analyze sentiment
            polarity, subjectivity = analyze_sentiment(tweet_text)
            
            # Load: Insert into database
            cur.execute(insert_query, (tweet_text, polarity, subjectivity))
        
        # Commit all the inserts to the database
        conn.commit()
        print(f"Successfully processed and inserted {len(df)} rows into the database.")

    except FileNotFoundError:
        print(f"Error: The file {CSV_FILE_PATH} was not found. Please check the path and filename.")
    except Exception as e:
        print(f"An error occurred: {e}")
        # Rollback any changes if an error occurs
        if conn:
            conn.rollback()
    finally:
        # --- CLOSE CONNECTION ---
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()