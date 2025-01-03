from sqlitecloud import connect
from datetime import datetime
import time
import random  # Import random module

# Connection string
connection_string = "sqlitecloud://cpqaniphnz.sqlite.cloud:8860/real_time?apikey=mXuVdgxgvcPxVZwCNbU51zrtfxZVQdA2RWhpdwXhfs4"

# Function to insert data into 'stream' table every second for 30 minutes
def insert_data_every_second():
    try:
        # Connect to the SQLite Cloud database
        conn = connect(connection_string)
        cursor = conn.cursor()

        # Start time tracking
        start_time = datetime.now()

        # Loop for 30 minutes (1800 seconds)
        while (datetime.now() - start_time).seconds < 1800:  # 30 minutes
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Current timestamp
            calls = random.randint(100, 999)  # Generate a random 3-digit number for calls

            # SQL INSERT query
            insert_query = "INSERT INTO stream (date, calls) VALUES (?, ?)"
            cursor.execute(insert_query, (current_time, calls))  # Use parameterized query

            # Commit the transaction
            conn.commit()
            print(f"Data inserted successfully: {current_time}, {calls}")

            # Wait for 1 second before inserting again
            time.sleep(1)

    except Exception as e:
        print("Failed to insert data:", e)

    finally:
        if 'conn' in locals() and conn:
            conn.close()

# Run the data insertion every second
if __name__ == "__main__":
    insert_data_every_second()
