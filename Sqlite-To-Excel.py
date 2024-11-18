import sqlite3
import pandas as pd
import math

# Database and Excel file details
DATABASE_FILE = "Products.db"
EXCEL_FILE = "Products_Export.xlsx"
ROW_LIMIT = 65530  # Excel's limit for URLs per worksheet

def export_to_excel_split(db_file, excel_file, row_limit=ROW_LIMIT):
    """Export SQLite database to multiple Excel sheets if data exceeds limits."""
    try:
        # Connect to the database
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Get all table names in the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        # Initialize a Pandas Excel writer
        with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
            for table_name in tables:
                table_name = table_name[0]
                # Load the table into a Pandas DataFrame
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

                # Calculate the number of sheets needed
                num_sheets = math.ceil(len(df) / row_limit)

                for sheet_num in range(num_sheets):
                    start_row = sheet_num * row_limit
                    end_row = start_row + row_limit

                    # Create a subset of the DataFrame for the current sheet
                    df_subset = df.iloc[start_row:end_row]

                    # Write the DataFrame to a new sheet
                    sheet_name = f"{table_name}_{sheet_num + 1}"
                    df_subset.to_excel(writer, sheet_name=sheet_name, index=False)

                    print(f"Exported rows {start_row} to {end_row} of table '{table_name}' to sheet '{sheet_name}'")

        print(f"Data successfully exported to {excel_file}")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the database connection
        conn.close()

if __name__ == "__main__":
    export_to_excel_split(DATABASE_FILE, EXCEL_FILE)
