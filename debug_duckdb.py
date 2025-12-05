import threading
import duckdb
import time
import os
from trilogy import Dialects
from duckdb_engine import DuckDBIdentifierPreparer

# The function that will be executed by each thread.
def run_duckdb_query(thread_id):
    """
    1. Creates an isolated in-memory DuckDB connection.
    2. Executes a static query.
    3. Fetches and prints the results.
    4. Closes the connection.
    """
    # Use a local variable for connection. ':memory:' ensures a fresh, in-memory DB for each thread.
    conn = None
    try:
        print(f"--- Thread {thread_id} ({threading.get_ident()}): Starting...")
        
        # 1. Create an in-memory DuckDB database connection
        # Crucial: Each thread must use its own connection object for thread safety.
        conn = Dialects.DUCK_DB.default_executor()
        # conn = duckdb.connect(database=':memory:', read_only=False)

        # 2. Define and execute a static select statement
        query = f"SELECT {thread_id} as thread_id, 'Data from Thread {thread_id}' as message, current_date() as query_time;"
        
        # Execute the query
        result = conn.execute_text(query)[-1]
        # result = conn.execute(query)
        
        # 3. Fetch results
        data = result.fetchall()
        
        # Print results from this specific thread
        print(f"--- Thread {thread_id}: Query executed successfully.")
        print(f"--- Thread {thread_id}: Result: {data}")

    except Exception as e:
        print(f"--- Thread {thread_id}: An error occurred: {e}")
        
    finally:
        # 4. Close the connection
        if conn:
            conn.close()
            print(f"--- Thread {thread_id}: Connection closed. Finishing.")


def main():
    """
    Main function to initialize and run the threads.
    """
    num_threads = 100
    threads = []
    
    print(f"Starting execution with {num_threads} threads...")

    # Create and start the threads
    for i in range(1, num_threads + 1):
        # Create a Thread object, target is the function, args are the arguments for the function
        thread = threading.Thread(target=run_duckdb_query, args=(i,))
        threads.append(thread)
        thread.start()
        print(f"Main: Thread {i} started.")

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    print("\nAll threads have completed their execution.")

if __name__ == "__main__":
    main()