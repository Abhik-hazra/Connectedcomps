# -*- coding: utf-8 -*-
"""Nodes to group.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1PWkfrUuZf7LsJeJsKfn3jZ_Ms7phg-8K
"""

import pandas as pd

# Assuming your DataFrame has two columns named 'column1' and 'column2'
# Replace these column names with your actual column names.

def create_adjacency_list(df):
    adjacency_list = {}
    for _, row in df.iterrows():
        node1, node2 = row['column1'], row['column2']
        if node1 not in adjacency_list:
            adjacency_list[node1] = []
        if node2 not in adjacency_list:
            adjacency_list[node2] = []
        adjacency_list[node1].append(node2)
        adjacency_list[node2].append(node1)
    return adjacency_list

def depth_first_search(node, adjacency_list, visited, component):
    visited.add(node)
    component.append(node)
    for neighbor in adjacency_list[node]:
        if neighbor not in visited:
            depth_first_search(neighbor, adjacency_list, visited, component)

def find_connected_components(df):
    adjacency_list = create_adjacency_list(df)
    visited = set()
    connected_components = []

    for node in adjacency_list:
        if node not in visited:
            component = []
            depth_first_search(node, adjacency_list, visited, component)
            connected_components.append(component)

    return connected_components

df = pd.read_csv("Sample.csv")

# Example usage
# Assuming 'df' is your pandas DataFrame containing two columns 'column1' and 'column2'

# connected_components will store the list of connected components.
connected_components = find_connected_components(df)
print(connected_components)

# Making it into groups 
df = pd.DataFrame([[x, i] for i, l in enumerate(connected_components, start=1) for x in l],
                  columns=['Pty', 'Group'])

## For making a group, even the below code work with concat, but less efficient 
df = pd.DataFrame(columns=['Pty', 'Groups'])
i = 1
for l in connected_components:
    new_df = pd.DataFrame(l, columns=['Pty'])
    new_df['Groups'] = i
    df = pd.concat([df,new_df], axis = 0)
    i = i + 1
