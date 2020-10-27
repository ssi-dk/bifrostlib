import os
import math
import pymongo
import atexit
import datetime
import json
import bson
import traceback
from typing import List, Set, Dict, Tuple, Optional

def date_now() -> datetime.datetime:
    """Get the current time as a datetime

    Note: 
        Needed to keep the same date in python and mongo, as mongo rounds to millisecond

    Returns:
        Current time rounded to miliseconds
    """
    current_time = datetime.datetime.utcnow()
    return current_time.replace(microsecond=math.floor(current_time.microsecond/1000)*1000)

CONNECTION = None
def get_connection() -> pymongo.mongo_client.MongoClient:
    """Get a connection to the DB

    Other Parameters:
        CONNECTION (pymongo.MongoClient): GLOBAL storing connection
        BIFROST_DB_KEY: (ENV) This is a environmental variable taken from the system

    Returns:
        pymongo.mongo_client.MongoClient: Sets global CONNECTION string based on env var BIFROST_DB_KEY

    Raises:
        ValueError: If DB is not set properly
    """
    global CONNECTION
    if CONNECTION is not None:
        return CONNECTION
    else:
        if os.getenv("BIFROST_DB_KEY", None) is not None:
            CONNECTION = pymongo.MongoClient(os.getenv("BIFROST_DB_KEY"))  # Note none here apparently will use defaults which means localhost:27017
            return CONNECTION
        else:
            raise ValueError("BIFROST_DB_KEY not set")

def close_connection():
    """Closes DB connection

    Other Parameters:
        CONNECTION (pymongo.MongoClient): GLOBAL storing connection
    """
    global CONNECTION
    if CONNECTION is not None:
        CONNECTION.close()

atexit.register(close_connection)

def pluralize(name: str) -> str:
    """Turns a string into a pluralized form. For example sample -> samples and property -> properties

    Args:
        name (str): A non plural string to turn into it's plural

    Returns:
        str: The pluralized form of the string.
    """
    if name.endswith("y"):
        return name[:-1]+"ies"
    else:
        return name+"s"

def json_to_bson(json_object: Dict) -> Dict:
    """Converts a json dict to bson dict

    Args:
        json_object (Dict): A json formatted dict

    Returns:
        Dict: A bson formatted dict
    """
    return bson.json_util.loads(json.dumps(json_object))

def bson_to_json(bson_object: Dict) -> Dict:
    """Converts a bson dict to json dict

    Args:
        bson_object (str): A bson formatted dict

    Returns:
        Dict: A json formatted dict
    """
    return json.loads(bson.json_util.dumps(bson_object))

def load(object_type:str, reference: Dict) -> Dict:
    """Loads an object based on it's id from the DB

    Note: 
        Inputs and outputs are json dict but database works on bson dicts

    Args: 
        object_type (str): A bifrost object type found in the database as a collection
        _id (Dict): json formatted objectid {"$oid": <value>}

    Returns: 
        Dict: json formatted dict of the object

    Raises:
        AssertionError: If db contains a duplicate _id
    """
    try:
        connection = get_connection()
        db = connection.get_database()
        collection_name = pluralize(object_type)
        if collection_name not in db.list_collection_names():
            return {}
        else:
            bson_id = json_to_bson({"_id": reference["_id"]})
            query = bson_id
            query_result = list(db[collection_name].find(query))
            assert(len(query_result)<=1)
            if len(query_result) == 0:
                return {}
            else:
                return bson_to_json(query_result[0])
    except AssertionError as error:
        print(error)
    except Exception as error:
        print(traceback.format_exc())
        return {}


def save(object_type, object_value: Dict) -> Dict:
    """Saves a object to the DB

    Note: 
        Inputs and outputs are json dict but database works on bson dicts

    Args: 
        object_type: A bifrost object type found in the database as a collection
        object_value: json formatted object

    Returns: 
        Dict: json formatted dict of the object with objectid

    Raises:
        KeyError: If object_type not in DB
    """
    try:
        connection = get_connection()
        db = connection.get_database()
        collection_name = pluralize(object_type)
        if collection_name not in db.list_collection_names():
            raise KeyError(f"collection name: {collection_name} not in DB")
        else:
            if "_id" in object_value:
                inserted_object = db[collection_name].find_one_and_update(
                    filter={"_id": object_value["_id"]},
                    update={"$set": object_value},
                    return_document=pymongo.ReturnDocument.AFTER,  # return new doc if one is upserted
                    upsert=True  # This might change in the future  # insert the document if it does not exist
                )
            else:
                result = db[collection_name].insert_one(object_value)
                object_value["_id"] = result.inserted_id
            return object_value
    except Exception:
        print(traceback.format_exc())
        return []

def delete(object_type, _id: Dict) -> bool:
    """Deletes a object from the DB based on it's id

    Note:
        This only removes the objects and doesn't handle dependencies or dangling objects

    Args:
        object_type (str): A bifrost object type found in the database as a collection
        _id (Dict): json formatted objectid {"$oid": <value>}

    Returns:
        bool: Successfully deleted | Failure to delete

    Raises:
        KeyError: If object_type not in DB
    """
    try:
        connection = get_connection()
        db = connection.get_database()
        collection_name = pluralize(object_type)
        if collection_name not in db.list_collection_names():
            raise KeyError(f"collection name: {collection_name} not in DB")
        else:
            deleted = db[collection_name].delete_one({"_id": _id})
            return deleted.deleted_count
    except Exception:
        print(traceback.format_exc())
        return False

