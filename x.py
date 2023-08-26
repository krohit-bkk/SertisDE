import argparse

def main():
    parser = argparse.ArgumentParser(description="Process data.")
    parser.add_argument("--source", required=True, help="Path to the source file")
    parser.add_argument("--database", required=True, help="Database name for inserting data")
    parser.add_argument("--table", required=True, help="Table name for inserting data")
    parser.add_argument("--save_mode", required=False, choices=["append", "overwrite"], help="Save mode choices: append|overwrite")

    save_mode = "append"
    default_save_mode = True
    
    args = parser.parse_args()

    source_path = args.source
    database_name = args.database
    table_name = args.table
    
    if args.save_mode is not None:
       default_save_mode = False
       save_mode = args.save_mode
    msg = "[default]" if default_save_mode else "[user provided]"

    # Your code to process data goes here
    print(f">>>> Processing data from: {source_path}")
    print(f">>>> Database: {database_name}")
    print(f">>>> Table: {table_name}")
    print(f">>>> Save mode: {save_mode} {msg}")


if __name__ == "__main__":
    main()
