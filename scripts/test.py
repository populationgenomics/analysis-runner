#!/usr/bin/env python3
from metamist.graphql import gql, query

my_query = gql("""
    {
        myProjects {
            id
            name
        }
    }
""")

result = query(my_query)
print(result)
