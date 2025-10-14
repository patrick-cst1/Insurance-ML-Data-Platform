from azure.cosmos import CosmosClient, exceptions
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast
from typing import List, Dict, Optional
import logging


def connect_cosmos(
    endpoint: str,
    key: str,
    database_name: str,
    container_name: str
) -> tuple:
    """
    Connect to Azure Cosmos DB container.
    
    Args:
        endpoint: Cosmos DB endpoint URL
        key: Cosmos DB primary or read-only key
        database_name: Database name
        container_name: Container name
    
    Returns:
        Tuple of (CosmosClient, database, container)
    """
    client = CosmosClient(endpoint, key)
    database = client.get_database_client(database_name)
    container = database.get_container_client(container_name)
    
    return client, database, container


def query_cosmos(
    container,
    query: str,
    parameters: Optional[List[Dict]] = None,
    enable_cross_partition: bool = True,
    max_items: Optional[int] = None
) -> List[Dict]:
    """
    Execute Cosmos DB SQL query.
    
    Args:
        container: Cosmos container client
        query: SQL query string
        parameters: Query parameters
        enable_cross_partition: Enable cross-partition query
        max_items: Maximum number of items to return
    
    Returns:
        List of query results
    """
    query_params = parameters if parameters else []
    
    items = list(container.query_items(
        query=query,
        parameters=query_params,
        enable_cross_partition_query=enable_cross_partition,
        max_item_count=max_items
    ))
    
    return items


def enrich_dataframe(
    spark: SparkSession,
    df: DataFrame,
    cosmos_endpoint: str,
    cosmos_key: str,
    cosmos_database: str,
    cosmos_container: str,
    join_keys: List[str],
    select_columns: Optional[List[str]] = None,
    broadcast_cosmos: bool = True
) -> DataFrame:
    """
    Enrich DataFrame with Cosmos DB data (batch read + join).
    
    Args:
        spark: SparkSession instance
        df: DataFrame to enrich
        cosmos_endpoint: Cosmos DB endpoint URL
        cosmos_key: Cosmos DB access key
        cosmos_database: Cosmos database name
        cosmos_container: Cosmos container name
        join_keys: List of join keys
        select_columns: Columns to select from Cosmos (None = all)
        broadcast_cosmos: Whether to broadcast Cosmos data (for small datasets)
    
    Returns:
        Enriched DataFrame
    """
    # Use Spark Cosmos DB connector for reading
    cosmos_config = {
        "spark.cosmos.accountEndpoint": cosmos_endpoint,
        "spark.cosmos.accountKey": cosmos_key,
        "spark.cosmos.database": cosmos_database,
        "spark.cosmos.container": cosmos_container,
        "spark.cosmos.read.inferSchema.enabled": "true"
    }
    
    # Read Cosmos data
    cosmos_df = spark.read.format("cosmos.oltp") \
        .options(**cosmos_config) \
        .load()
    
    # Select required columns
    if select_columns:
        cosmos_df = cosmos_df.select(*join_keys, *select_columns)
    
    # Broadcast join if Cosmos data is small
    if broadcast_cosmos:
        cosmos_df = broadcast(cosmos_df)
    
    # Left join to enrich original DataFrame
    enriched_df = df.join(
        cosmos_df,
        on=join_keys,
        how="left"
    )
    
    return enriched_df


def batch_read_by_keys(
    container,
    partition_key_field: str,
    id_field: str,
    keys: List[tuple],
    batch_size: int = 100
) -> List[Dict]:
    """
    Batch read Cosmos items by (partition_key, id) pairs (point read optimization).
    
    Args:
        container: Cosmos container client
        partition_key_field: Partition key field name
        id_field: ID field name
        keys: List of (partition_key_value, id_value) tuples
        batch_size: Batch size for reading
    
    Returns:
        List of retrieved items
    """
    results = []
    
    for i in range(0, len(keys), batch_size):
        batch = keys[i:i + batch_size]
        for pk, item_id in batch:
            try:
                item = container.read_item(item=item_id, partition_key=pk)
                results.append(item)
            except exceptions.CosmosResourceNotFoundError:
                logging.warning(f"Item not found: pk={pk}, id={item_id}")
                continue
    
    return results
