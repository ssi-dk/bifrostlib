import os
import pymongo
import atexit
import json
from bson import json_util
import traceback
from typing import Dict
import gridfs
import magic
import sys
from pymongo import collection

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
    return json_util.loads(json.dumps(json_object))


def bson_to_json(bson_object: Dict) -> Dict:
    """Converts a bson dict to json dict

    Args:
        bson_object (str): A bson formatted dict

    Returns:
        Dict: A json formatted dict
    """
    return json.loads(json_util.dumps(bson_object))


def remove_id(reference: Dict) -> Dict:
    if "_id" in reference:
        reference.pop("_id")
    return reference


def load(object_type: str, reference: Dict) -> Dict:
    """Loads an object based on it's id from the DB

    Note: 
        Inputs and outputs are json dict but database works on bson dicts

    Args: 
        object_type (str): A bifrost object type found in the database as a collection
        reference (Dict): json formatted reference (normally id as objectid {"$oid": <value>} and name)

    Returns: 
        Dict: json formatted dict of the object

    Raises:
        AssertionError: If db contains a duplicate _id or name
    """
    try:
        connection = get_connection()
        db = connection.get_database()
        collection_name = pluralize(object_type)
        bson_reference = json_to_bson(reference)
        if collection_name not in db.list_collection_names():
            return remove_id(reference)
        else:
            if bson_reference.get("_id", None) is not None:
                query = ({"_id": bson_reference["_id"]})
            elif bson_reference.get("name", None) is not None:
                query = ({"name": bson_reference["name"]})
            else:
                return remove_id(reference)
            query_result = list(db[collection_name].find(query))
            assert(len(query_result) <= 1)
            if len(query_result) == 0:
                return remove_id(reference)
            else:
                return bson_to_json(query_result[0])
    except AssertionError as error:
        print(error)
    except Exception as error:
        print(traceback.format_exc())
        return remove_id(reference)


def save(object_type: str, object_value: Dict) -> Dict:
    """Saves a object to the DB

    Note: 
        Inputs and outputs are json dict but database works on bson dicts

    Args: 
        object_type (str): A bifrost object type found in the database as a collection
        object_value (Dict): json formatted object

    Returns: 
        Dict: json formatted dict of the object with objectid
    """
    try:
        connection = get_connection()
        db = connection.get_database()
        collection_name = pluralize(object_type)

        bson_object_value = json_to_bson(object_value)
        
        if collection_name not in db.list_collection_names():
            db[collection_name]
        if "_id" in bson_object_value:
            inserted_object = db[collection_name].find_one_and_update(
                filter={"_id": bson_object_value["_id"]},
                update={"$set": bson_object_value},
                return_document=pymongo.ReturnDocument.AFTER,  # return new doc if one is upserted
                upsert=True  # This might change in the future  # insert the document if it does not exist
            )
        else:
            result = db[collection_name].insert_one(bson_object_value)
            bson_object_value["_id"] = result.inserted_id
        return bson_to_json(bson_object_value)
    except Exception:
        print(traceback.format_exc())
        sys.exit()
        return []


def delete(object_type: str, reference: Dict) -> bool:
    """Deletes a object from the DB based on it's id

    Note:
        This only removes the objects and doesn't handle dependencies or dangling objects

    Args:
        object_type (str): A bifrost object type found in the database as a collection
        reference (Dict): json formatted reference (normally id as objectid {"$oid": <value>} and name)

    Returns:
        bool: Successfully deleted | Failure to delete

    Raises:
        KeyError: If object_type not in DB
    """
    try:
        connection = get_connection()
        db = connection.get_database()
        collection_name = pluralize(object_type)
        bson_reference = json_to_bson(reference)
        if collection_name not in db.list_collection_names():
            raise KeyError(f"collection name: {collection_name} not in DB")
        else:
            if bson_reference.get("_id", None) is not None:
                deleted = db[collection_name].delete_one({"_id": bson_reference["_id"]})
            elif bson_reference.get("name", None) is not None:
                deleted = db[collection_name].delete_one({"name": bson_reference["name"]})
            else:
                return False
            return deleted.deleted_count
    except Exception:
        print(traceback.format_exc())
        return False


def save_file(_id, _name, _type, file_path) -> str:
    try:
        connection = get_connection()
        db = connection.get_database()
        fs = gridfs.GridFS(db)

        # check if file is there
        existing = fs.find_one({
            "_id": _id,
            "full_path": file_path
        })
        if existing:
            print(("WARNING: File {} already exists in".format(file_path),
                   " the db for this component,",
                   " it was overwritten by the new file."), file=sys.stderr)
            fs.delete(existing.id)

        mimetype = magic.from_file(file_path, mime=True)

        with open(file_path, 'rb') as file_handle:
            file_id = fs.put(file_handle,
                             _id=_id,
                             name=_name,
                             type=_type,
                             full_path=file_path,
                             filename=os.path.basename(file_path),
                             mimetype=mimetype)
        return file_id
    except Exception:
        print(traceback.format_exc())
        return None


def load_file(file_id, save_to_path=None, subpath=False) -> str:
    try:
        connection = get_connection()
        db = connection.get_database()
        fs = gridfs.GridFS(db)

        fobj = fs.get(file_id)

        if save_to_path is None:
            if subpath:
                save_to_path = fobj.full_path
            else:
                save_to_path = fobj.filename
        elif os.path.isdir(save_to_path):
            if subpath:
                save_to_path = os.path.join(save_to_path, fobj.full_path)
            else:
                save_to_path = os.path.join(save_to_path, fobj.filename)

        if os.path.isfile(save_to_path):
            raise FileExistsError

        dirname = os.path.dirname(save_to_path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        with open(save_to_path, 'wb') as file_handle:
            file_handle.write(fobj.read())
        return file_id
    except Exception:
        print(traceback.format_exc())
        return None


def find_files(object_id):
    connection = get_connection()
    db = connection.get_database()
    fs = gridfs.GridFS(db)
    return list(fs.find({"_id": object_id}))
