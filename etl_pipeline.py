import os
import pandas as pd
import requests
import sqlite3
from bs4 import BeautifulSoup

# Define URLs and filenames
urls = {
    "10-K_2020": "https://www.sec.gov/Archives/edgar/data/320193/000032019320000096/aapl-20200926.htm",
    "10-K_2021": "https://www.sec.gov/Archives/edgar/data/320193/000032019321000105/aapl-20210925.htm",
    "10-K_2022": "https://www.sec.gov/Archives/edgar/data/320193/000032019322000108/aapl-20220924.htm",
    #"10-K_2023": "",
}
headers = {"User-Agent": "YourName your.email@example.com"}
table_styles = [
    "border-collapse:collapse;display:inline-table;vertical-align:top;width:100.000%",
    "border-collapse:collapse;display:inline-table;margin-bottom:5pt;vertical-align:text-bottom;width:100.000%",
]
span_texts = [
    "CONSOLIDATED STATEMENTS OF OPERATIONS",
    "CONSOLIDATED STATEMENTS OF CASH FLOWS",
    "CONSOLIDATED BALANCE SHEETS"
]

# Download and extract the ZIP file
def download_files():
    # Create a "bronze" folder if it doesn't exist
    bronze_folder = "bronze"
    os.makedirs(bronze_folder, exist_ok=True)

    # Download and save files
    for name, url in urls.items():
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        if response.status_code == 200:
            file_path = os.path.join(bronze_folder, f"{name}.html")
            with open(file_path, "w", encoding="utf-8") as file:
                file.write(response.text)
            print(f"Saved {name} to {file_path}")
        else:
            print(f"Failed to download {name}. HTTP Status Code: {response.status_code}")

def parse_files():
    # Create the silver folder
    silver_folder = "silver"
    os.makedirs(silver_folder, exist_ok=True)

    for name, url in urls.items():
        print(f"Processing URL: {url}")

        # Fetch the page content
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # Parse the HTML with BeautifulSoup
        soup = BeautifulSoup(response.content, "html.parser")

        for span_text in span_texts:
            print(f"Searching for section: {span_text}")
            span = soup.find("span", string=span_text)

            if span:
                # Look for the corresponding table after each span
                for style in table_styles:
                    next_table = span.find_next("table", {"style": style})
                    
                    if next_table:
                        # Extract rows and cells from the table
                        rows = next_table.find_all("tr")
                        table_data = []

                        for row in rows:
                            cells = row.find_all(["td", "th"])  # Include headers if available
                            row_data = [cell.get_text(separator=" ", strip=True) or None for cell in cells]
                            table_data.append(row_data)

                        # Convert to a pandas DataFrame
                        df = pd.DataFrame(table_data)

                        # Handle empty or duplicate headers
                        df = df.dropna(how="all").reset_index(drop=True)  # Drop empty rows
                        if not df.empty:
                            first_row = df.iloc[0]
                            if first_row.is_unique:  # Use the first row as header if unique
                                df.columns = first_row
                                df = df[1:]  # Drop the first row
                            else:  # Generate unique headers if duplicates or missing
                                df.columns = [f"Column_{i}" for i in range(len(df.columns))]

                        # Replace None or empty strings with pd.NA
                        df = df.replace({None: ""})

                        # Save to Parquet file with dynamic name
                        file_name = f"{span_text.replace(' ', '_').lower()}_{name.replace('-','')}.parquet"
                        file_path = os.path.join(silver_folder, file_name)
                        df.to_parquet(file_path, engine="pyarrow", index=False)
                        print(f"Saved table to: {file_path}")

def load_parquet_to_sqlite(parquet_file, db_file, table_name):
    """
    Loads a Parquet file into an SQLite database.

    Args:
        parquet_file (str): Path to the Parquet file.
        db_file (str): Path to the SQLite database file.
        table_name (str): Name of the table in the SQLite database.
    """
    # Read the Parquet file into a Pandas DataFrame
    df = pd.read_parquet(parquet_file)

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)

    # Check if the table exists
    try:
        conn.execute(f"SELECT * FROM {table_name} LIMIT 1")
        print(f"Table {table_name} already exists. Appending data...")
        existing_data = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        df = pd.concat([existing_data, df]).drop_duplicates().reset_index(drop=True)
    except sqlite3.OperationalError:
        print(f"Table {table_name} does not exist. Creating...")

    # Write the DataFrame to the SQLite database
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

def save_table_to_parquet(db_file, table_name, output_folder):
    """
    Saves a table from an SQLite database to a Parquet file in the gold folder.

    Args:
        db_file (str): Path to the SQLite database file.
        table_name (str): Name of the table in the SQLite database.
        output_folder (str): Path to the gold folder for saving the Parquet file.
    """
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)

    # Read the table into a Pandas DataFrame
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

    # Save the DataFrame as a Parquet file
    # output_file = os.path.join(output_folder, f"{table_name}.parquet")
    # df.to_parquet(output_file, engine="pyarrow", index=False)
    # print(f"Saved {table_name} to {output_file}")

    conn.close()

def validate_table_creation(db_file, table_name):
    """
    Validates if the table exists in the SQLite database.

    Args:
        db_file (str): Path to the SQLite database file.
        table_name (str): Name of the table to validate.
    """
    conn = sqlite3.connect(db_file)
    try:
        # Check if the table exists
        df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 10", conn)
        print(df)
        print(f"Validation successful: Table {table_name} exists in {db_file}.")
    except sqlite3.OperationalError:
        print(f"Validation failed: Table {table_name} does not exist in {db_file}.")
    finally:
        conn.close()                        

# Main workflow
def main():
    download_files()
    parse_files()
    silver_path = './silver'
    parquet_files = [os.path.join(silver_path, f) for f in os.listdir(silver_path) if os.path.isfile(os.path.join(silver_path, f))]
    print(parquet_files)
    
    gold_folder = "./gold"
    os.makedirs(gold_folder, exist_ok=True)

    # Process each file
    for file in parquet_files:
        table_name = os.path.splitext(os.path.basename(file))[0]
        db_file = os.path.join(gold_folder, f"{table_name}.db")  # Save each DB in the gold folder
        
        load_parquet_to_sqlite(file, db_file, table_name)
        save_table_to_parquet(db_file, table_name, gold_folder)
        validate_table_creation(db_file, table_name)

if __name__ == "__main__":
    main()