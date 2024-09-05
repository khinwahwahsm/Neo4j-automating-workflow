# import dependent modules
import pandas as pd

# extract data
def extract_data(filepath: object) -> object:
    """
       :param filepath: str, file path to CSV data
       :output: pandas dataframe, extracted from CSV data
    """
    try:
        # Read the CSV file and store it in a dataframe
        df = pd.read_csv(filepath)

    # Handle exception if any of the files are missing
    except FileNotFoundError as e:
        print(f"Error: {e}")

    # Handle any other exceptions
    except Exception as e:
        print(f"Error: {e}")

    return df

# transform data
def transform_data(df: object) -> object:
    """
       Simple Transformation Function
       :param df: pandas dataframe, extracted data
       :output: pandas dataframe, transformed data
    """
    # delete all rows where functionName is not "submit".
    df = df[df['functionName']=='submit']

    # Keep columns '_id', 'addedAtUtc', 'environmentId', 'eventName', 'userId', 'flowId', 
    # 'flowOriginId', 'companyId', 'dianaResourceId', 'stepId', 'subscriptionEntities.entityId',
    # 'subscriptionEntities.entityCompanyId', and remove the rest.
    df = df.loc[:,
    ['_id', 'addedAtUtc', 'environmentId', 'eventName', 'userId', 'flowId', 'flowOriginId',
      'companyId', 'dianaResourceId', 'stepId', 'subscriptionEntities.entityId',
        'subscriptionEntities.entityCompanyId']
        ]
    
    # Rename columns '_id', 'addedAtUtc', 'subscriptionEntities.entityId', 'subscriptionEntities.entityCompanyId'
    df=df.rename(columns={
        '_id':'event_id',
        'addedAtUtc':'date',
        'subscriptionEntities.entityId':'Invited_userId',
        'subscriptionEntities.entityCompanyId':'Invited_companyId'})

    # drop duplicate rows
    df = df.drop_duplicates()

    #df.fillna(' ',inplace=True)
    df.dropna(inplace=True)
    
    # convert columns to appropriate data types
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].dt.strftime("%Y-%m-%d %H:%M:%S")

    # reset index
    df.reset_index(drop=True, inplace=True)

    return df

# load data
def load_data(df: object) -> object:
    '''
    Steps:
        1. To prevent duplicates, create user_dict to store both raising_user_node and invited_user_node, and create event_dict to store event_node.
        2. Create a list to store all nodes and a list to store all relationships.
        3. For each raising_userId, create 'raised' relationships between rasing_users and events.
        4. For each event, create 'submitted_to' relationships between events and invited_users, and create 'invited' relationships between raising_users and invited_users.
        5. Upload the nodes and relationships to neo4j database.
    '''   
    # Import necessary classes from Py2neo library
    from py2neo import Node, Relationship, Graph, Subgraph

    # Establish connection to a Neo4j graph database
    graph = Graph("bolt://localhost:7687", auth=('neo4j', '12345678'), name='neo4j')

    # Initialize dictionaries to store unique users and events, and a list for all nodes
    user_dict = {}
    event_dict = {}
    node_list = []

    # Iterate over each row in the DataFrame to process user and event information
    for i in range(len(df)):
        # Extract user and event IDs from the DataFrame
        raising_userId = df['userId'][i]
        invited_userId = df['Invited_userId'][i]
        eventId = df['event_id'][i]

        # Create node for the user who is raising the event
        raising_user_node = Node(
            'user',
            id=raising_userId,
            company_id=df['companyId'][i]
        )

        # Create node for the user who is invited to the event
        invited_user_node = Node(
            'user',
            id=invited_userId,
            company_id=df['Invited_companyId'][i]
        )

        # Create node for the event itself
        event_node = Node(
            'event',
            id=eventId,
            event_name=df['eventName'][i],
            date=df['date'][i],
            step_id=df['stepId'][i],
            environment_id=df['environmentId'][i]
        )

        # Store the nodes in dictionaries to avoid duplication
        user_dict[raising_userId] = raising_user_node
        user_dict[invited_userId] = invited_user_node
        event_dict[eventId] = event_node

    # Combine all unique user and event nodes into one list
    user_node_list = list(user_dict.values())
    event_node_list = list(event_dict.values())
    node_list.extend(user_node_list)
    node_list.extend(event_node_list)

    # Initialize a list to store all relationships between nodes
    relationship_list = []

    # Iterate over each unique user to process relationships with events and invited users
    for i in range(len(df['userId'].unique())):
        userId = list(df.groupby('userId', sort=False))[i][0]
        data = list(df.groupby('userId', sort=False))[i][1]
        data.reset_index(drop=True, inplace=True)
        raising_user_node = user_dict[userId]

        for j in range(len(data['event_id'].unique())):
            eventId = list(data.groupby('event_id',sort=False))[j][0]
            data2 = list(data.groupby('event_id',sort=False))[j][1]
            data2.reset_index(drop=True, inplace=True)
            event_node = event_dict[eventId]

            raised = Relationship(raising_user_node, 'raised', event_node)
            relationship_list.append(raised)

            for r in range(len(data2)):
                invited_userId = data2['Invited_userId'][r]
                invited_user_node = user_dict[invited_userId]

                submitted = Relationship(event_node, 'submitted_to', invited_user_node)
                invited = Relationship(raising_user_node, 'invited', invited_user_node)

                relationship_list.append(submitted)
                relationship_list.append(invited)
    
    # Remove duplicates from relationship_list
    relationship_list = list(set(relationship_list))

    # Create a subgraph with all nodes and relationships and add it to the Neo4j graph
    graph.create(Subgraph(node_list, relationship_list))    

# clear the database
def delete_data() -> object:

    from py2neo import Graph

    graph = Graph('http://localhost:7474',auth=('neo4j','12345678'), name = 'neo4j') # connect to the database
    batch_size = 10000
    total_deleted = 0

    while True:
        # Delete a batch of nodes and return the count of deleted nodes
        deleted = graph.run(f"MATCH (n) WITH n LIMIT {batch_size} DETACH DELETE n RETURN count(n) AS deleted").evaluate()
        if not deleted:
            break
        total_deleted += deleted
        print(f"Deleted {total_deleted} nodes so far")
    
    graph.delete_all()
    print("Finished deleting nodes.")

# etl pipeline
def run_pipeline(filepath: object) -> object:
    # Step 1: Extract data
    df = extract_data(filepath)
    # Step 2: Transform data
    df = transform_data(df)
    # Step 3: Load data
    load_data(df)