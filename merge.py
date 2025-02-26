import pandas as pd

def merge_gender_served(value1, value2):
    #Merge gender_served by combining unique values
    values = set(str(value1).split('; ') + str(value2).split('; '))
    return '; '.join(values) if values else None

def merge_christian_tradition(value1, value2):
    #Merge christian_tradition by keeping the most frequent value
    return value1 if value1 == value2 else f"{value1}; {value2}" if value1 and value2 else value1 or value2

def merge_religious_family(value1, value2):
    #Merge religious_family by combining unique values
    values = set(str(value1).split('; ') + str(value2).split('; '))
    return '; '.join(values) if values else None

def merge_start_end_date(value1, value2):
    #Merge start and end date fields by keeping the earliest start date and latest end date
    if value1 and value2:
        return min(value1, value2) if 'start' in value1 else max(value1, value2)
    return value1 or value2

def merge_notes(value1, value2):
    #Merge notes by appending both values with a separator
    return f"{value1} | {value2}" if value1 and value2 else value1 or value2

def merge_source(value1, value2):
    #Merge source by combining unique values
    values = set(str(value1).split('; ') + str(value2).split('; '))
    return '; '.join(values) if values else None


def read_and_split_csv(file_path, delimiter='@'):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    data = [line.strip().split(delimiter) for line in lines]
    df = pd.DataFrame(data[1:], columns=data[0])
    return df


def merge_rows(row1, row2, all_columns):
    # Merge two rows based on predefined merge strategies
    merged_row = { 
        'gender_served': merge_gender_served(row1.get('gender_served'), row2.get('gender_served')),
        'christian_tradition': merge_christian_tradition(row1.get('christian_tradition'), row2.get('christian_tradition')),
        'religious_family': merge_religious_family(row1.get('religious_family'), row2.get('religious_family')),
        'start_day': merge_start_end_date(row1.get('start_day'), row2.get('start_day')),
        'start_month': merge_start_end_date(row1.get('start_month'), row2.get('start_month')),
        'start_year': merge_start_end_date(row1.get('start_year'), row2.get('start_year')),
        'end_day': merge_start_end_date(row1.get('end_day'), row2.get('end_day')),
        'end_month': merge_start_end_date(row1.get('end_month'), row2.get('end_month')),
        'end_year': merge_start_end_date(row1.get('end_year'), row2.get('end_year')),
        'notes': merge_notes(row1.get('notes'), row2.get('notes')),
        'source': merge_source(row1.get('source'), row2.get('source'))
    }
    
    # Add non-merged columns
    merged_row.update({key: value for key, value in row1.items() if key not in merged_row})
    
    # Reorder to better compare
    return {key: merged_row.get(key, None) for key in all_columns}

def main():
    df_duplicates = pd.read_csv("Institution Duplication Check - Sheet1_checked.csv", encoding="latin1")
    df_nodes = read_and_split_csv("chcd_v2.5_nodes.csv")
    
    # Add merged_chcd_id column if it doesn't exist
    if 'merged_chcd_id' not in df_nodes.columns:
        df_nodes['merged_chcd_id'] = None  # Initialize the column with None values

    all_columns = df_nodes.columns.tolist()
    merged_data = []
    
    for _, row in df_duplicates.iterrows():
        # Check if 'ID1' or 'merged_chcd_id' exists in 'df_nodes'
        matching_node1 = df_nodes[(df_nodes['chcd_id'] == row['ID1']) | (df_nodes['merged_chcd_id'] == row['ID1'])]
        matching_node2 = df_nodes[(df_nodes['chcd_id'] == row['ID2']) | (df_nodes['merged_chcd_id'] == row['ID2'])]

        if not matching_node1.empty and not matching_node2.empty:
            node1 = matching_node1.iloc[0].to_dict()
            node2 = matching_node2.iloc[0].to_dict()

            # Immediately update merged_chcd_id before merging
            df_nodes.loc[df_nodes['chcd_id'] == row['ID2'], 'merged_chcd_id'] = row['ID1']
            df_nodes.loc[df_nodes['merged_chcd_id'] == row['ID2'], 'merged_chcd_id'] = row['ID1']

            # Now merge the rows and store in merged_data
            merged_data.append(merge_rows(node1, node2, all_columns))
            
            # Remove the second duplicate row from df_nodes
            df_nodes = df_nodes[df_nodes['chcd_id'] != row['ID2']]  # remove row2 (the duplicate)
            print(f"{row['ID1']} and {row['ID2']} merged")
        else:
            print(f"Warning: No matching node found for IDs {row['ID1']} or {row['ID2']}")
    
    # Add the merged rows back to df_nodes
    merged_df = pd.DataFrame(merged_data)
    df_nodes = pd.concat([df_nodes, merged_df], ignore_index=True)

    # Save the modified df_nodes
    df_nodes.to_csv("chcd_v2.5_nodes_modified.csv", index=False)
    print("Merging complete. Output saved to chcd_v2.5_nodes_modified.csv")

    # Save the merged data to a separate file (merged data before being added to df_nodes)
    merged_df.to_csv("merged_output.csv", index=False)
    print("Merged data saved to merged_output.csv")

if __name__ == "__main__":
    main()