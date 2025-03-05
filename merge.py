import pandas as pd

def merge_id(value1, value2):
    return value1

def merge_name_western(value1, value2):
    if value1 and value2:
        return value1 if len(value1) <= len(value2) else value2
    return value1 or value2

def merge_alternative_name_western(value1, value2):
    values = set(str(value1).split('; ') + str(value2).split('; '))
    return '; '.join(values) if values else None

def merge_chinese_name_hanzi(value1, value2):
    if value1 and value2:
        return value1 if len(value1) <= len(value2) else value2
    return value1 or value2

def merge_alternative_chinese_name_hanzi(value1, value2):
    values = set(str(value1).split('; ') + str(value2).split('; '))
    return '; '.join(values) if values else None

def merge_name_romanized(value1, value2):
    if value1 and value2:
        return value1 if len(value1) <= len(value2) else value2
    return value1 or value2

def merge_alternative_name_romanized(value1, value2):
    values = set(str(value1).split('; ') + str(value2).split('; '))
    return '; '.join(values) if values else None

def merge_institution_category(value1, value2):
    return value1

def merge_institution_subcategory(value1, value2):
    return value1

def merge_nationality(value1, value2):
    return value1 if value1 == value2 else "FLAG_FOR_REVIEW"

def merge_gender_served(value1, value2):
    values = set(str(value1).split('; ') + str(value2).split('; '))
    return '; '.join(values) if values else None

def merge_christian_tradition(value1, value2):
    return value1 if value1 == value2 else f"{value1}; {value2}" if value1 and value2 else value1 or value2

def merge_religious_family(value1, value2):
    values = set(str(value1).split('; ') + str(value2).split('; '))
    return '; '.join(values) if values else None

def merge_start_end_date(value1, value2):
    if value1 and value2:
        return min(value1, value2) if 'start' in value1 else max(value1, value2)
    return value1 or value2

def merge_notes(value1, value2):
    return f"{value1} | {value2}" if value1 and value2 else value1 or value2

def merge_source(value1, value2):
    values = set(str(value1).split('; ') + str(value2).split('; '))
    return '; '.join(values) if values else None

def read_and_split_csv(file_path, delimiter='@'):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    data = [line.strip().split(delimiter) for line in lines]
    df = pd.DataFrame(data[1:], columns=data[0])
    return df

def merge_rows(row1, row2, all_columns):
    merged_row = {
        'id': merge_id(row1.get('id'), row2.get('id')),
        'name_western': merge_name_western(row1.get('name_western'), row2.get('name_western')),
        'alternative_name_western': merge_alternative_name_western(row1.get('alternative_name_western'), row2.get('alternative_name_western')),
        'chinese_name_hanzi': merge_chinese_name_hanzi(row1.get('chinese_name_hanzi'), row2.get('chinese_name_hanzi')),
        'alternative_chinese_name_hanzi': merge_alternative_chinese_name_hanzi(row1.get('alternative_chinese_name_hanzi'), row2.get('alternative_chinese_name_hanzi')),
        'name_romanized': merge_name_romanized(row1.get('name_romanized'), row2.get('name_romanized')),
        'alternative_name_romanized': merge_alternative_name_romanized(row1.get('alternative_name_romanized'), row2.get('alternative_name_romanized')),
        'institution_category': merge_institution_category(row1.get('institution_category'), row2.get('institution_category')),
        'institution_subcategory': merge_institution_subcategory(row1.get('institution_subcategory'), row2.get('institution_subcategory')),
        'nationality': merge_nationality(row1.get('nationality'), row2.get('nationality')),
         
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
    merged_row.update({key: value for key, value in row1.items() if key not in merged_row})
    return {key: merged_row.get(key, None) for key in all_columns}

def main():
    df_duplicates = pd.read_csv("Institution Duplication Check - Sheet1_checked.csv", encoding="latin1")
    df_nodes = read_and_split_csv("chcd_v2.5_nodes.csv")

    all_columns = df_nodes.columns.tolist()
    merged_data = []
    id_replacement = []
    id_map = {}  # Track merged IDs: maps old ID to new ID

    for _, row in df_duplicates.iterrows():
        # Resolve current IDs considering previous merges
        current_id1 = row['ID1']
        while current_id1 in id_map:
            current_id1 = id_map[current_id1]
        
        current_id2 = row['ID2']
        while current_id2 in id_map:
            current_id2 = id_map[current_id2]

        # Skip if already merged
        if current_id1 == current_id2:
            print(f"Skipping duplicate pair {row['ID1']} and {row['ID2']} (already merged into {current_id1})")
            continue

        # Check existence in nodes
        matching_node1 = df_nodes[df_nodes['chcd_id'] == current_id1]
        matching_node2 = df_nodes[df_nodes['chcd_id'] == current_id2]

        if not matching_node1.empty and not matching_node2.empty:
            node1 = matching_node1.iloc[0].to_dict()
            node2 = matching_node2.iloc[0].to_dict()

            merged_row = merge_rows(node1, node2, all_columns)
            merged_data.append(merged_row)

            # Record replacement and update id_map
            id_replacement.append({'ID Replaced': current_id2, 'ID Now': current_id1})
            id_map[row['ID2']] = current_id1  # Map original ID2 to current_id1
            id_map[current_id2] = current_id1  # Also map resolved ID2 to current_id1

            # Remove the merged node
            df_nodes = df_nodes[df_nodes['chcd_id'] != current_id2]
            print(f"Merged {current_id1} and {current_id2} (originally {row['ID1']} and {row['ID2']})")
        else:
            print(f"Warning: No matching nodes for {current_id1} or {current_id2}")

    # Update df_nodes with merged data
    merged_df = pd.DataFrame(merged_data)
    df_nodes = pd.concat([df_nodes, merged_df], ignore_index=True)

    # Save outputs
    df_nodes.to_csv("chcd_v2.5_nodes_modified.csv", index=False)
    merged_df.to_csv("merged_output.csv", index=False)
    pd.DataFrame(id_replacement).to_csv("id_replacements.csv", index=False)
    print("All files saved successfully.")

if __name__ == "__main__":
    main()