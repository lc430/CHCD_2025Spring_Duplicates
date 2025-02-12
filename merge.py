import pandas as pd
from py2neo import Graph

def check_nodes(graph, node1, node2):
    query = f"""
    MATCH (n1:Institution), (n2:Institution)
    WHERE n1.chcd_id = "{node1}" AND n2.chcd_id = "{node2}"
    RETURN n1, n2
    """
    result = graph.run(query).data()
    return result  

def merge_nodes(graph, node1, node2):
    query = f"""
    MATCH (n1:Institution {{chcd_id: "{node1}"}}), (n2:Institution {{chcd_id: "{node2}"}})
    
    // merge by keeping the existing values and append different info
    SET n1 += apoc.map.clean(apoc.map.merge(n1, n2), [null, ""], [])
    SET n1.notes = COALESCE(n1.notes, '') + " Merged with node {node2}"
    
    WITH n1, n2
    CALL apoc.map.entries(n1) YIELD key, value
    SET n1.notes = n1.notes + key + ": " + apoc.convert.toString(value) + "; "
    WITH n1, n2
    CALL apoc.map.entries(n2) YIELD key, value
    SET n1.notes = n1.notes + key + ": " + apoc.convert.toString(value) + "; "

    // merge relationships
    WITH n1, n2
    CALL apoc.refactor.mergeNodes([n1, n2]) YIELD node
    
    RETURN node
    """
    graph.run(query)
    print(f"Merged {node1} with {node2}")


def main():
    print("hello")
    count = 0
    graph = Graph("neo4j@bolt://localhost:7687", auth=("neo4j", "chcd1234"))

    file = "Institution Duplication Check - Sheet1.csv"  
    df = pd.read_csv(file, encoding="latin1")


    df_filtered = df[df["Duplicates"] == True]  # Filter true duplicates
    print(f"Found {len(df_filtered)} duplicate records to process.")


    for index, row in df.iterrows():
        node1 = row["ID1"]
        node2 = row["ID2"]
        print(node1, node2)

        if check_nodes(graph, node1, node2):
            merge_nodes(graph, node1, node2)
            count += 1
        else:
            print(f"Skipping {node1} and {node2}: One or both not found.")
    
    print(f"{count} merges completed")

if __name__ == "__main__":
    main()
