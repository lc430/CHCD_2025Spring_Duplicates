# CHCD_2025Spring_Duplicates

This project aims to use the confirmed duplicated nodes from the Institution Duplication Check file to indicate and merge them in the chcd_v2.5_nodes.csv. (versions should not matter as long as it is separated by deliminator @)

The 'Institution Duplication Check - Sheet1.csv' file is the list of all potential duplicated nodes by automated python script(CHCD_2024Summer_DuplicatesCheck).

The 'Institution Duplication Check - Sheet1_checked.csv' file is the filtered list that only includes confirmed duplicates by manual.

The 'chcd_v2.5_nodes.csv' file is the data from CHCD database with duplicates.

The 'chcd_v2.5_nodes_modified.csv' file is the final output of the data that duplicates has been removed.

The 'id_replacements.csv' file would be the record of the id transformations of those merged nodes. This may be a reference for future relationships merging of the data.

The 'merge.py' is the python script merging the institution duplicates.

The 'merged_output.csv' file is the list of merged nodes.

##### Mar 5 2025 final commit from Lize&Eugene:
Institution duplication merging finished!

##### Mar 5 2025 commit from Lize:
duplicated id replaced in the check file

##### Feb 26 2025 commit from Lize:
second half of merging strategies almost done

the replacement of chcd_ids in the institution duplication check file left to do

##### Feb 12 2025 first commit from Lize:
Test from local database
Found 53 duplicate records to process
47 merges completed

##### Merge results not completely checked yet
Only checked whether one of the merged notes deleted from the database(✅), but its notes remains **null** (not sure whether other columns of info successfully appended). 
(not sure how to restore the neo4j database previous version so...)

##### Further things left to do:
check with local database again to make sure the merge function is correct or not? 
if correct, may employ the function to the remote database
if not, debug or further edit the script <3 
