import pandas as pd

def merge_rows(row1, row2):
    merged = {}
    
    # Keep the first ID as the representative ID
    merged["id"] = row1["id"]
    
    # Choose the first available category and subcategory
    merged["institution_category"] = row1["institution_category"]
    merged["institution_subcategory"] = row1["institution_subcategory"]
    
    return merged

# Function to process CSV file and merge duplicates
def process_csv(input_file, output_file):
    # Removed the Neo4j connection since the merging should no longer be handled 
    # within Neo4j, but instead produce a new CSV file with the duplicates removed
    df = pd.read_csv(input_file, encoding="latin1")
    
    merged_data = []
    seen_pairs = set()
    
    # Iterate through each row to identify and merge duplicates
    for index, row in df.iterrows():
        node1 = row["ID1"]
        node2 = row["ID2"]
        
        if (node1, node2) in seen_pairs or (node2, node1) in seen_pairs:
            continue  # Skip already processed pairs
        
        duplicate_rows = df[(df["ID1"] == node1) & (df["ID2"] == node2)]
        
        if len(duplicate_rows) > 1:
            # Merge the duplicate rows based on the defined strategy
            merged_row = merge_rows(duplicate_rows.iloc[0], duplicate_rows.iloc[1])
            merged_data.append(merged_row)
            seen_pairs.add((node1, node2))
        else:
            merged_data.append(row)
    
    # Save the merged data to a new CSV file
    merged_df = pd.DataFrame(merged_data)
    merged_df.to_csv(output_file, index=False, encoding="latin1")
    print(f"Merged data saved to {output_file}")

if __name__ == "__main__":
    input_csv = "Institution Duplication Check - Sheet1.csv"
    output_csv = "Merged_Institutions.csv"
    process_csv(input_csv, output_csv)
