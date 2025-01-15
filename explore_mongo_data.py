from pymongo import MongoClient
from pprint import pprint
from typing import Optional, Dict, List, Any

class MongoExplorer:
    def __init__(self, connection_string: str, database_name: str):
        """
        Initialize MongoDB connection
        
        Args:
            connection_string: MongoDB connection string
            database_name: Name of the database to explore
        """
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]
    
    def list_collections(self) -> List[str]:
        """List all collections in the database"""
        return self.db.list_collection_names()
    
    def get_collection_stats(self, collection_name: str) -> Dict[str, Any]:
        """
        Get basic statistics about a collection
        
        Returns dictionary with count, size, and sample document
        """
        collection = self.db[collection_name]
        stats = {
            'document_count': collection.count_documents({}),
            'sample_document': collection.find_one(),
            'fields': self.get_field_names(collection_name)
        }
        return stats
    
    def get_field_names(self, collection_name: str) -> List[str]:
        """Get all field names used in the collection"""
        collection = self.db[collection_name]
        sample_docs = collection.find().limit(100)
        fields = set()
        for doc in sample_docs:
            fields.update(doc.keys())
        return sorted(list(fields))
    
    def query_collection(self, collection_name: str, query: Dict = None, 
                        limit: int = 5) -> List[Dict]:
        """
        Query documents from a collection
        
        Args:
            collection_name: Name of collection to query
            query: MongoDB query dictionary (default: None, returns all documents)
            limit: Maximum number of documents to return
        """
        collection = self.db[collection_name]
        if query is None:
            query = {}
        return list(collection.find(query).limit(limit))
    
    def explore_database(self):
        """Print overview of database structure and contents"""
        print("\n=== Database Overview ===")
        collections = self.list_collections()
        print(f"\nFound {len(collections)} collections:")
        
        for collection_name in collections:
            print(f"\n--- Collection: {collection_name} ---")
            if collection_name =="dialog": 
                collection = self.db[collection_name]
                cursor = collection.find().limit(10)
                print(list(cursor))
            # stats = self.get_collection_stats(collection_name)
            # print(f"Document count: {stats['document_count']}")
            # print("\nFields available:")
            # for field in stats['fields']:
            #     print(f"  - {field}")
            # print("\nSample document:")
            # pprint(stats['sample_document'])

# Example usage
if __name__ == "__main__":
    # Replace with your connection details
    CONNECTION_STRING = "mongodb://localhost:27017/"
    DATABASE_NAME = "chatgpt_telegram_bot"
    
    explorer = MongoExplorer(CONNECTION_STRING, DATABASE_NAME)
    
    # Print database overview
    explorer.explore_database()
    
    # Example: Query specific collection
    collection_name = "your_collection"
    query = {"field_name": "value"}  # Replace with your query
    results = explorer.query_collection(collection_name, query, limit=5)
    print("\nQuery results:")
    pprint(results)