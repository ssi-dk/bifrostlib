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
    """
    Needed to keep the same date in python and mongo, as mongo rounds to millisecond
    """
    current_time = datetime.datetime.utcnow()
    return current_time.replace(microsecond=math.floor(current_time.microsecond/1000)*1000)

CONNECTION = None
def get_connection() -> pymongo.mongo_client.MongoClient:
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
    global CONNECTION
    if CONNECTION is not None:
        CONNECTION.close()

atexit.register(close_connection)

# json id {"_id": {"$oid": <value>}}

def pluralize(name: str) -> str:
    """Turns a string into a pluralized form. For example sample -> samples and property -> properties

    Args:
        name: a non plural string to turn into it's plural
    Returns:
        The pluralized form of the string.
    """
    if name.endswith("y"):
        return name[:-1]+"ies"
    else:
        return name+"s"

def json_to_bson(json_object: Dict) -> Dict:
    """Converts a json dict to bson dict

    Args:
        json_object: a json formatted dict
    Returns:
        A bson formatted dict
    """
    return bson.json_util.loads(json.dumps(json_object))

def bson_to_json(bson_object: Dict) -> Dict:
    """Converts a bson dict to json dict

    Args:
        bson_object - a bson formatted dict
    Returns:
        a json formatted dict
    """
    return json.loads(bson.json_util.dumps(bson_object))

def load(object_type:str, id: Dict) -> Dict:
    """Load a object based on it's id from the DB
    NOTE: Inputs and outputs are json dict but database works on bson dicts

    Args: 
        object_type: A bifrost object type found in the database as a collection
        id: json formatted objectid {"$oid": <value>}
    Returns: 
        json formatted dict of the object
    """
    try:
        connection = get_connection()
        db = connection.get_database()
        collection_name = pluralize(object_type)
        if collection_name not in db.list_collection_names():
            return {}
        else:
            bson_id = json_to_bson(id)
            query = {"_id": bson_id}
            query_result = list(db[collection_name].find(query))
            if len(query_result) == 0:
                return {}
            elif len(query_result) == 1:
                return bson_to_json(query_result[0])
            else:
                raise Exception(f"{object_type} _id: {query} exists multiple times in DB")
    except Exception:
        print(traceback.format_exc())
        return {}


def save(object_type, object_value: Dict) -> Dict:
    """Save a object based to the DB
    NOTE: Inputs and outputs are json dict but database works on bson dicts

    Args: 
        object_type: A bifrost object type found in the database as a collection
        object_value: json formatted object
    Returns: 
        json formatted dict of the object with objectid
    """
    try:
        connection = get_connection()
        db = connection.get_database()
        collection_name = pluralize(object_type)
        if collection_name not in db.list_collection_names():
            raise Exception(f"collection name: {collection_name} not in DB")
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

def delete(object_type, object_value: json.object):
    return object
