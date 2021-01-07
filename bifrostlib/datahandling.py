# I'll split out all the objects to their own files
import traceback
import sys
from bifrostlib import database_interface
import os
import json
import jsmin
import warlock
import pandas
import functools
import datetime
import math
from typing import Any, List, Dict, Union


global BIFROST_SCHEMA
BIFROST_SCHEMA = None


def date_now() -> datetime.datetime:
    """Get the current time as a datetime

    Note: 
        Needed to keep the same date in python and mongo, as mongo rounds to millisecond and has a T

    Returns:
        Current time rounded to miliseconds, converts the time to be usable in mongoDB
    """
    current_time = datetime.datetime.utcnow()
    current_time = current_time.replace(microsecond=math.floor(current_time.microsecond/1000)*1000)
    current_time_in_json = {"$date": str(current_time)[:-3].replace(" ","T")}
    return current_time_in_json

def has_a_database_connection() -> bool:
    """Checks if you have a database connection

    Returns:
        bool: True - has a connection, False - No connection established
    """
    try:
        connection = database_interface.get_connection()
        connection.server_info()
        return True
    except:
        print(traceback.format_exc())
        return False

def load_schema() -> Dict:
    """loads BIFROST_SCHEMA from bifrost.jsonc which is the basis for objects

    Other Parameters:
        BIFROST_SCHEMA (dict): GLOBAL storing the BIFROST_SCHEMA

    Returns:
        Dict: json formatted schema
    """
    global BIFROST_SCHEMA
    if BIFROST_SCHEMA == None:
        with open(os.path.join(os.path.dirname(__file__),"schemas/bifrost.jsonc"), "r") as file_stream:
            minified_bifrost_schema = jsmin.jsmin(file_stream.read())
        BIFROST_SCHEMA = json.loads(minified_bifrost_schema)
    return BIFROST_SCHEMA

def get_schema_object(object_type: str, schema_version: str) -> Dict:
    """Get a object schema from the BIFROST_SCHEMA
    
    Note:
        With how it's organized references and datatypes need to be included with object

    Args:
        object_type (str): object type based on available objects in schema
        schema_version (str, optional): Schema version from json schema (bifrost.jsonc). Defaults to "v2_1_0".

    Other Parameters:
        BIFROST_SCHEMA (dict): GLOBAL storing the BIFROST_SCHEMA

    Returns:
        Dict: The object schema with datatypes schema and references schema it may need
    """
    BIFROST_SCHEMA = load_schema()
    object_schema = BIFROST_SCHEMA.get("definitions", {}).get("objects", {}).get(object_type, {}).get(schema_version, {})
    object_schema["definitions"] = {}
    object_schema["definitions"]["datatypes"] = BIFROST_SCHEMA.get("definitions", {}).get("datatypes", {})
    object_schema["definitions"]["references"] = BIFROST_SCHEMA.get("definitions", {}).get("references", {})
    object_schema["definitions"]["objects"] = BIFROST_SCHEMA.get("definitions", {}).get("objects", {})
    return object_schema

def get_schema_datatypes(datatype: str) -> Dict:
    """Get a datatype schema from the BIFROST_SCHEMA

    Args:
        datatype (str): get a bifrost datatype from the schema

    Other Parameters:
        BIFROST_SCHEMA (dict): GLOBAL storing the BIFROST_SCHEMA

    Returns:
        Dict: The datatype schema
    """
    BIFROST_SCHEMA = load_schema()
    object_schema = BIFROST_SCHEMA.get("definitions", {}).get("datatypes", {}).get("bifrost", {}).get(datatype, {})
    object_schema["definitions"] = {}
    object_schema["definitions"]["datatypes"] = BIFROST_SCHEMA.get("definitions", {}).get("datatypes", {})
    return object_schema

def get_schema_reference(reference_type: str, schema_version: str) -> Dict:
    """Get the reference schema of from the BIFROST_SCHEMA

    Args:
        reference_type (str): The reference type matching the associated object
        schema_version (str, optional): Schema version from json schema (bifrost.jsonc). Defaults to "v2_1_0".

    Returns:
        Dict: Reference schema as json
    """
    BIFROST_SCHEMA = load_schema()
    object_schema = BIFROST_SCHEMA.get("definitions", {}).get("references", {}).get(reference_type, {}).get(schema_version, {})
    object_schema["definitions"] = {}
    object_schema["definitions"]["datatypes"] = BIFROST_SCHEMA.get("definitions", {}).get("datatypes", {})
    return object_schema


class BifrostObjectDataType(Dict):
    """For schema datatypes

    Args:
        Dict: Extended off a base dict type to allow easier retrieval and setting of json items.
    """
    _data_type: str = None # to be set in inherited classes to match the inherited type
    def __init__(self, value: Dict) -> None:
        """Initialize object based on it's json schema

        Args:
            datatype (str): datatype that can be found in the json schema
            _json (Dict): json data to be validated against the schema
        """
        schema: Dict = get_schema_datatypes(self._data_type)
        self._model = warlock.model_factory(schema)
        self._json = {}
        self._json = self._model(value)
    def __repr__(self) -> str:
        """Returns the validated json as a string

        Returns:
            str: json
        """
        return str(self._json)
    def __getitem__(self, key: str) -> Any:
        """Get item

        Args:
            key (str): json key

        Returns:
            Any: associated key value
        """
        return self._json[key]
    def __setitem__(self, key: str, value: Any) -> None:
        """Set item

        Args:
            key (str): json key
            value (Any): intended value for key, will only work if it's valid against the schema
        """
        self._json[key] = value
    def __delitem__(self, key: str) -> None:
        """Delete item

        Args:
            key (str): json sub object to remove
        """
        self._json.__delitem__(key)
    @property
    def json(self) -> Dict:
        """Get the json as a dict

        Returns:
            Dict: copy of the json contents
        """
        return self._json.copy()

class Version(BifrostObjectDataType):
    """Version object in bifrost. 

    Args:
        BifrostObjectDataType: Inherited data type
    """
    _data_type: str = "version"

    def __init__(self, schema_version: str="v2_1_0", value: Dict=None, code_version: str=None, resource_version: str=None, ) -> None:
        """Initialization of version object

        Args:
            schema_version (str, optional): Schema version from json schema (bifrost.jsonc). Defaults to "v2_1_0".
            value (Dict, optional): Pre loaded data that's valid against the schema. Defaults to None.
            code_version (str, optional): Version of code for associated components. Defaults to None.
            resource_version (str, optional): Latest resource version for associated resources in components. Defaults to None. e.g. Resistance DB is routinely updated and works without code changes, need to capture this.
        """
        if value is None:
            value = {
                "schema": [schema_version]
            }
            if code_version is not None:
                value.update({"code": code_version})
            if resource_version is not None:
                value.update({"resources": resource_version})
        BifrostObjectDataType.__init__(self, value)
    def add_schema_version(self, schema_version: str) -> None:
        """Add additional schema version

        Args:
            schema_version (str, optional): Schema version from json schema (bifrost.jsonc). Defaults to "v2_1_0".
        """
        if schema_version not in self._json["schema"]:
            self._json["schema"].append(schema_version)

class Metadata(BifrostObjectDataType):
    """Metadata object of an entry (created at/updated at).

    Args:
        BifrostObjectDataType: Inherited data type
    """
    _data_type: str = "metadata"
    def __init__(self, value: Dict=None) -> None:
        """Initialization

        Args:
            value (Dict, optional): Base values for metadata. Defaults to None. Metadata is currently composed of a created_at and updated_at datetime. Sets times to when object is created.
        """
        if value is None:
            value = {
                "created_at": date_now(),
                "updated_at": date_now()
            }
        BifrostObjectDataType.__init__(self, value)
    def updated_now(self) -> None:
        """Updates the updated_at field to now, done with saving objects
        """
        self._json["updated_at"] = date_now()

class Test(BifrostObjectDataType):
    _data_type: str = "test"

    def __init__(self, _value: Dict = None, name: str = None, display_name: str = None, effect: str = None, value: str = None, status: str = None, reason: str = None) -> None:
        if _value is None:
            _value = {
            }
            if name is not None:
                _value.update({"name": name})
            if display_name is not None:
                _value.update({"display_name": display_name})
            if effect is not None:
                _value.update({"effect": effect})
            if value is not None:
                _value.update({"value": value})
            if status is not None:
                _value.update({"status": status})
            if reason is not None:
                _value.update({"reason": reason})
        BifrostObjectDataType.__init__(self, _value)
class Requirements(BifrostObjectDataType):
    """Requirements object of an component (to check whether its already in meets the requirements).

    Args:
        BifrostObjectDataType: Inherited data type
    """
    _data_type: str = "requirements"
    def __init__(self, value: Dict=None) -> None:
        """Initialization

        Args:
            value (Dict, optional): Base values for metadata. Defaults to None. Metadata is currently composed of a created_at and updated_at datetime. Sets times to when object is created.
        """
        self._data_type: str="metadata" 
        if value is None:
            value = {}
        BifrostObjectDataType.__init__(self, value)
class BifrostObjectReference(Dict):
    """Base object for references, all references are based off of _id and name

    Args:
        Dict: Extended off a base dict type to allow easier retrieval and setting of json items.
    """
    _reference_type: str = None

    def __init__(self, schema_version: str, value: Dict = None, _id: str = None, name: str = None) -> None:
        """Initialize a reference which is either a _id or name, if both then _id is used and name ignored. 

        Note: 
            Either name or _id is required to work properly, this can be provided as the individual vars on in the value. Value allows you to also overload the reference with additional values.
        Args:
            schema_version (str): Schema version from json schema (bifrost.jsonc). 
            _id (str, optional): A unique DB identifier . Defaults to None.
            name (str, optional): A unique name for the DB object. Defaults to None.
            value (Dict, optional): Pass additional values. Defaults to {}.
        """
        self.schema_version = schema_version
        schema: Dict = get_schema_reference(self._reference_type, self.schema_version)
        self._model = warlock.model_factory(schema)
        entry = {"name": ""}
        if _id is not None:
            entry.update({"_id": {"$oid":_id}}) # expects id as a string converts to json
        if name is not None:
            entry.update({"name": name})
        if value is not None:
            entry.update(value)
        self._json = self._model(entry)
    def __repr__(self) -> str:
        """Returns the validated json as a string

        Returns:
            str: json
        """
        return str(self._json)
    def __getitem__(self, key:str ) -> Any:
        """Get item

        Args:
            key (str): json key

        Returns:
            Any: associated key value
        """
        return self._json[key]
    def __setitem__(self, key: str, value: Any) -> None:
        """Set item

        Args:
            key (str): json key
            value (Any): intended value for key, will only work if it's valid against the schema
        """
        self._json[key] = value
    def __delitem__(self, key: str) -> None:
        """Delete item

        Args:
            key (str): json sub object to remove
        """
        self._json.__delitem__(key)
    @property
    def json(self) -> Dict:
        """Get the json as a dict

        Returns:
            Dict: copy of the json contents
        """
        return self._json.copy()
    @property
    def reference_type(self) -> str:
        """Get the reference type

        Returns:
            str: reference type
        """
        return self._reference_type

class BifrostObject(Dict):
    """Base object for bifrost objects. Id's are not required for creation.

    Args:
        Dict: Extended off a base dict type to allow easier retrieval and setting of json items.
    """
    _object_type: str = None # to be set in inherited classes to match the inherited type
    def __init__(self, schema_version: str, value: Dict = {}):
        """Initialization

        Args:
            schema_version (str): schema version of the object to look up under that object ttype
            value (Dict, optional): the initial entry for the json which will be validated. Defaults to {}.
        """
        self.schema_version = schema_version
        schema: Dict = get_schema_object(self._object_type, self.schema_version)
        self._model = warlock.model_factory(schema)
        self._json = self._model(value)
        if "metadata" not in self._json:
            self._json["metadata"] = Metadata().json
        if "version" not in self._json:
            self._json["version"] = Version(schema_version=self.schema_version).json
    def __repr__(self) -> str:
        """Returns the validated json as a string

        Returns:
            str: json
        """
        return str(self._json)
    def __getitem__(self, key: str) -> Any:
        """Get item

        Args:
            key (str): json key

        Returns:
            Any: associated key value
        """
        return self._json[key]
    def __setitem__(self, key:str , value: Any) -> None:
        """Set item

        Args:
            key (str): json key
            value (Any): intended value for key, will only work if it's valid against the schema
        """
        self._json[key] = value
    def __delitem__(self, key:str) -> None:
        """Delete item

        Args:
            key (str): json sub object to remove
        """
        self._json.__delitem__(key)
    @property
    def json(self) -> Dict:
        """Get the json as a dict

        Returns:
            Dict: copy of the json contents
        """
        return self._json.copy()
    @json.setter
    def json(self, value: Dict) -> None:
        """Attempts to set the json to a schema valid dict

        Returns:
            Dict: copy of the json contents
        """
        self._json = self._model(value)
    def update_json(self, value: Dict) -> None:
        """Attempts to update the json to add dict entries

        Returns:
            Dict: copy of the json contents
        """
        self._json.update(value)
    @classmethod
    def load(cls, reference: BifrostObjectReference):
        json_object: Dict = database_interface.load(cls._object_type, reference.json)
        if "_id" not in json_object:
            return None
        return cls(schema_version=reference.schema_version, value=json_object)

    def save(self) -> None:
        """Save the object to the DB

        Note:
            This updates the metadate update_at section
        """
        metadata = Metadata(value=self.json["metadata"])
        metadata.updated_now()
        self._json["metadata"] = metadata.json
        self._json = self._model(database_interface.save(self._object_type, self.json))
    def delete(self) -> bool:
        """Delete the object from the DB

        Returns:
            bool: True on successful deletion, False on failure to delete
        """
        return database_interface.delete(self._object_type, self.to_reference().json)
    def to_reference(self, additional_values: Dict = {}) -> BifrostObjectReference:
        """Returns the object as a reference object

        Args:
            additional_values (Dict, optional): If added values are needed in the reference. Defaults to {}.

        Returns:
            BifrostObjectReference: Reference to the object
        """
        entry = {}
        if self._json.get("_id") is not None:
            entry.update({"_id": self._json["_id"]})
        if self._json.get("name") is not None:
            entry.update({"name": self._json["name"]})
        entry.update(additional_values)
        return BifrostObjectReference(self.schema_version, entry)

class Category(BifrostObject):
    """Category object that stores values for common analysis. Not it's own document in the DB.

    Args:
        BifrostObject: Inherited data type
    """
    _object_type: str = "category"
    
    def __init__(self, schema_version = "v2_1_0", value: Dict = None, name: str = None):
        """Initialization

        Args:
            schema_version (str, optional): Schema version from json schema (bifrost.jsonc). Defaults to "v2_1_0".
            value (Dict, optional): The structure of the category. Defaults to None.
        """
        if value is None:
            value = {}
            if name is not None:
                value['name'] = name
        BifrostObject.__init__(self, schema_version, value)


class ComponentReference(BifrostObjectReference):
    """Component reference object

    Args:
        BifrostObjectReference: Inherited data type
    """
    _reference_type = "component"

    def __init__(self, schema_version: str = "v2_1_0", value: Dict = {}, _id: str = None, name: str = None):
        """Initialization

        Args:
            schema_version (str, optional): Schema version from json schema (bifrost.jsonc). Defaults to "v2_1_0".
            value (Dict, optional): json formatted values to be added. Defaults to {}.
            _id (str, optional): Unique id, if empty name is required to load properly, if name is also provided then id is used. Defaults to None.
            name (str, optional): Unique name, if empty id is required . Defaults to None.
        """
        BifrostObjectReference.__init__(self, schema_version, value, _id, name)


class Component(BifrostObject):  # Alternative name is pipeline
    """Component (aka Pipeline), these are functions to run across the data/analysis and determine what to store in bifrost

    Args:
        BifrostObject: Inherited data type
    """
    _object_type: str = "component"

    def __init__(self, schema_version:str ="v2_1_0", value: Dict = None, name: str = None):
        """Initialization

        Args:
            schema_version (str, optional): Schema version from json schema (bifrost.jsonc). Defaults to "v2_1_0".
            value ([type], optional): json values to initialize on. Defaults to None.
            name (str, optional): Unique name. Defaults to None.
        """
        if value is None:
            value = {}
            if name is not None:
                value['name'] = name
        BifrostObject.__init__(self, schema_version, value)


class SampleReference(BifrostObjectReference):
    """Sample reference object

    Args:
        BifrostObjectReference: Inherited data type
    """
    _reference_type = "sample"

    def __init__(self, schema_version: str = "v2_1_0", value: Dict = {}, _id: str = None, name: str = None):
        """Initialization

        Args:
            schema_version (str, optional): Schema version from json schema (bifrost.jsonc). Defaults to "v2_1_0".
            value (Dict, optional): json formatted values to be added. Defaults to {}.
            _id (str, optional): Unique id, if empty name is required to load properly, if name is also provided then id is used. Defaults to None.
            name (str, optional): Unique name, if empty id is required . Defaults to None.
        """
        BifrostObjectReference.__init__(self, schema_version, value, _id, name)

class Sample(BifrostObject): # Alternative name is genomicsample
    """Sample (aka genomic sample), these are the base unit of genomic WGS data that are stored in the system and get things run agaisnt them

    Args:
        BifrostObject: Inherited data type
    """
    _object_type: str = "sample"

    def __init__(self, schema_version:str = "v2_1_0", value: Dict = None, name: str = None) -> None:
        """Initialization

        Args:
            schema_version (str, optional): Schema version from json schema (bifrost.jsonc). Defaults to "v2_1_0".
            value ([type], optional): json values to initialize on. Defaults to None.
            name (str, optional): Unique name. Defaults to None.
        """
        self._object_type = "sample"
        if value is None:
            value = {
                "name": name, 
                "components": [], 
                "categories": {},
                "tags": []
            }
        BifrostObject.__init__(self, schema_version, value)
    @property
    def components(self) -> List[ComponentReference]:
        """get the components related to the sample

        Returns:
            List[ComponentReference]: All the samples related tosample
        """
        components = []
        for i in self._json["components"]:
            components.append(ComponentReference(value = i))
        return components
    @components.setter
    def components(self, components:List[ComponentReference]) -> None:
        """set the components related to the sample

        Args:
            components (List[ComponentReference]): The components you want to associate to the sample
        """
        json_items = []
        for i in components:
            json_items.append(i.json)
        self._json["components"] = json_items
    def set_component_status(self, component:ComponentReference, status: str) -> None:
        added = False
        for i in self._json["components"]:
            if component['name'] == i.get('name', None):
                i['status'] = status
                added = True
        if added == False:
            component['status'] = status
            self._json["components"].append(component.json)
    def get_category(self, key: str) -> Category:
        """get the category based on provided key

        Args:
            key (str): category you want to get the value of

        Returns:
            Category: A category object of the associated key, None if not found
        """
        try:
            return Category(value=self._json["categories"][key])
        except KeyError:
            return None
    def set_category(self, category: Category):
        """set the category based on a provided Category

        Args:
            category (Category): The category you want to set for the sample
        """
        for name in category.json:
            self._json["categories"][category["name"]] = category.json
    def add_tag(self, tag):
        self._json["tags"].append(tag)
    def remove_tag(self, tag: str):
        self._json["tags"].remove(tag)
class HostReference(BifrostObjectReference):
    """Host reference object

    Args:
        BifrostObjectReference: Inherited data type
    """
    _reference_type = "host"

    def __init__(self, schema_version: str = "v2_1_0", value: Dict = {}, _id: str = None, name: str = None):
        """Initialization

        Args:
            schema_version (str, optional): Schema version from json schema (bifrost.jsonc). Defaults to "v2_1_0".
            value (Dict, optional): json formatted values to be added. Defaults to {}.
            _id (str, optional): Unique id, if empty name is required to load properly, if name is also provided then id is used. Defaults to None.
            name (str, optional): Unique name, if empty id is required . Defaults to None.
        """
        BifrostObjectReference.__init__(self, schema_version, value, _id, name)
class Host(BifrostObject):
    """Host, a epidemiolgy based object

    Args:
        BifrostObject: Inherited data type
    """
    _object_type: str = "host"

    def __init__(self, schema_version: str = "v2_1_0", value: Dict = None, name: str = None):
        """Initialization

        Args:
            schema_version (str, optional): Schema version from json schema (bifrost.jsonc). Defaults to "v2_1_0".
            value ([type], optional): json values to initialize on. Defaults to None.
            name (str, optional): Unique name. Defaults to None.
        """
        self._object_type = "host"
        if value is None:
            value = {
                "name": name,
                "samples": []
            }
        BifrostObject.__init__(self, schema_version, value)
        @property
        def samples(self) -> List[SampleReference]:
            """get samples associated to the host

            Returns:
                List[SampleReference]: Sample references associated to run
            """
            hosts = []
            for i in self._json["samples"]:
                hosts.append(SampleReference(value=i))
            return hosts
        @samples.setter
        def samples(self, samples:List[SampleReference]) -> None:
            """set samples associated to the Host

            Args:
                samples (List[SampleReference]): Sample references associated to run
            """
            json_items = []
            for i in samples:
                json_items.append(i.json)
            self._json["samples"] = json_items

class RunReference(BifrostObjectReference):
    """Run reference object

    Args:
        BifrostObjectReference: Inherited data type
    """
    _reference_type = "run"

    def __init__(self, schema_version: str = "v2_1_0", value: Dict = {}, _id: str = None, name: str = None):
        """Initialization

        Args:
            schema_version (str, optional): Schema version from json schema (bifrost.jsonc). Defaults to "v2_1_0".
            value (Dict, optional): json formatted values to be added. Defaults to {}.
            _id (str, optional): Unique id, if empty name is required to load properly, if name is also provided then id is used. Defaults to None.
            name (str, optional): Unique name, if empty id is required . Defaults to None.
        """
        BifrostObjectReference.__init__(self, schema_version, value, _id, name)
class Run(BifrostObject): # Alternative name is collection
    """Run (aka collection), is a grouping of samples and hosts. Analysis is sometimes done on a group in context and sometimes it's just a organizational structure

    Args:
        BifrostObject: Inherited data type
    """
    _object_type: str = "run"

    def __init__(self, schema_version = "v2_1_0", value: Dict = None, name: str = None) -> None:
        """Initialization

        Args:
            schema_version (str, optional): Schema version from json schema (bifrost.jsonc). Defaults to "v2_1_0".
            value ([type], optional): json values to initialize on. Defaults to None.
            name (str, optional): Unique name. Defaults to None.
        """
        if value is None:
            value = {
                "name": name, 
                "samples": [], 
                "components": [], 
                "hosts": []
            }
        BifrostObject.__init__(self, schema_version, value)
    def sample_name_generator(self, name: str):
        if self._json["name"] is None:
            return None
        else:
            return f"{self._json['name']}___{name}"
    @property
    def samples(self) -> List[SampleReference]:
        """get samples associated to the run

        Returns:
            List[SampleReference]: Sample references associated to run
        """
        samples = []
        for i in self._json["samples"]:
            samples.append(SampleReference(value=i))
        return samples
    @samples.setter
    def samples(self, samples: List[SampleReference]) -> None:
        """set samples associated to the run

        Args:
            List[SampleReference]: Sample references associated to run
        """
        json_items = []
        for i in samples:
            json_items.append(i.json)
        self._json["samples"] = json_items
    @property
    def components(self) -> List[ComponentReference]:
        """get components associated to the run

        Returns:
            List[ComponentReference]: Component references associated to run
        """
        components = []
        for i in self._json["components"]:
            components.append(ComponentReference(value=i))
        return components
    @components.setter
    def components(self, components:List[ComponentReference]) -> None:
        """set components associated to the run

        Returns:
            List[ComponentReference]: Component references associated to run
        """
        json_items = []
        for i in components:
            json_items.append(i.json)
        self._json["components"] = json_items
    @property
    def hosts(self) -> List[HostReference]:
        """get hosts associated to the run

        Returns:
            List[HostReference]: Host references associated to run
        """
        hosts = []
        for i in self._json["hosts"]:
            hosts.append(HostReference(value=i))
        return hosts
    @hosts.setter
    def hosts(self, hosts:List[HostReference]) -> None:
        """set hosts associated to the run

        Returns:
            List[HostReference]: Host references associated to run
        """
        json_items = []
        for i in hosts:
            json_items.append(i.json)
        self._json["hosts"] = json_items


class SampleComponentReference(BifrostObjectReference):
    """SampleComponent reference object

    Args:
        BifrostObjectReference: Inherited data type
    """
    _reference_type = "sample_component"

    def __init__(self, schema_version: str = "v2_1_0", value: Dict = {}, _id: str = None, name: str = None):
        """Initialization

        Args:
            schema_version (str, optional): Schema version from json schema (bifrost.jsonc). Defaults to "v2_1_0".
            value (Dict, optional): json formatted values to be added. Defaults to {}.
            _id (str, optional): Unique id, if empty name is required to load properly, if name is also provided then id is used. Defaults to None.
            name (str, optional): Unique name, if empty id is required . Defaults to None.
        """
        BifrostObjectReference.__init__(self, schema_version, value, _id, name)
    @staticmethod
    def name_generator(sample_ref: SampleReference, component_ref: ComponentReference):
        if sample_ref["name"] is None or component_ref["name"] is None:
            return None
        else:
            return f"{sample_ref['name']}___{component_ref['name']}"

class SampleComponent(BifrostObject):
    """SampleComponent, aka result of a component on a sample, is the result object

    Args:
        BifrostObject: Inherited data type
    """
    _object_type: str = "sample_component"

    def __init__(self, schema_version:str = "v2_1_0", value: Dict = None, sample_reference:SampleReference = None, component_reference:ComponentReference = None) -> None:
        """Initializatiion

        Args:
            schema_version (str, optional): Schema version from json schema (bifrost.jsonc). Defaults to "v2_1_0".
            value (Dict, optional): json formatted values to be added. Defaults to {}.
            sample_reference (SampleReference, optional): Reference to sample. Defaults to None.
            component_reference (ComponentReference, optional): Reference to component. Defaults to None.
        """
        if value is None:
            value = {
                "categories": {},
                "results": {}
                }
        if sample_reference is not None:
            value.update({"sample": sample_reference.json})
        if component_reference is not None:
            value.update({"component": component_reference.json})
        if sample_reference is not None and component_reference is not None:
            value["name"] = SampleComponentReference.name_generator(sample_reference, component_reference)
        BifrostObject.__init__(self, schema_version, value)
    @property
    def sample(self) -> SampleReference:
        """get sample associated to the samplecomponent

        Returns:
            SampleReference: sample reference associated to samplecomponent
        """
        return SampleReference(value=self._json["sample"])
    @sample.setter
    def sample(self, sample:SampleReference) -> None:
        """set sample associated to the samplecomponent

        Args:
            SampleReference: sample reference associated to samplecomponent
        """
        self._json["sample"] = sample.json
        self.set_name()
    @property
    def component(self) -> ComponentReference:
        """get component associated to the samplecomponent

        Returns:
            ComponentReference: component reference associated to samplecomponent
        """
        return ComponentReference(value=self._json["component"])
    @component.setter
    def component(self, component:ComponentReference) -> None:
        """set component associated to the samplecomponent

        Args:
            ComponentReference: component reference associated to samplecomponent
        """
        self._json["component"] = component.json
        self.set_name()
    def set_name(self):
        self._json["name"] = SampleComponentReference.name_generator(self.sample(), self.component())
    @staticmethod
    def _has_requirement(object_json: Dict, requirement: Dict, expected_value: Union[None, str,List[str]]) -> bool:
        """[summary]

        Args:
            object_json (Dict): The object being checked on
            requirement (Dict): The requirement to be checked against
            expected_value (Union[None, str,List[str]]): The expected value, if value is None then requirement is met if key exists, if value is str then it must be that specific value, if value is List[str] then it can be any of the values in the list

        Returns:
            bool: True, it has the requirement | False, it doesn't have the requirement
        """
        try:
            value = functools.reduce(dict.get, requirement, object_json)
            if expected_value is None:
                print(f"[true] Requirement(value:{value}, expected_value: NA, requirement{requirement}", file=sys.stderr)
                return True
            elif type(expected_value) is not list:
                expected_value = [expected_value]
            if value in expected_value:
                print(f"[true] Requirement(value:{value}, expected_value:{expected_value}, requirement{requirement}", file=sys.stderr)
                return True
            else:
                print(f"[fail] Requirement(value:{value}, expected_value:{expected_value}, requirement{requirement}", file=sys.stderr)
                return False
        except Exception:
            print(f"[fail] Requirement(value:<Failed to retrieve>, expected_value:{expected_value}, requirement{requirement}", file=sys.stderr)
            return False
    def has_requirements(self) -> bool:
        """if samplecomponent has requirements as defined

        Returns:
            bool: True, it has all the requirement | False, it doesn't have all the requirement
        """
        
        component = Component.load(self.component)
        sample = Sample.load(self.sample)
        no_failures = True
        if component.json.get("requirements", {}) == None:
            return True
        sample_requirements = component.json.get("requirements", {}).get("sample", {})
        requirements = pandas.json_normalize(sample_requirements, sep=".").to_dict(orient='records')[0] # Converts the line from a dict to a 2D dataframe with 1 row, then store as a dict at sheet 0
        
        for requirement, expected_value in requirements.items():
            if not self._has_requirement(sample.json, requirement.split("."), expected_value):
                no_failures = False
        component_requirements = component.get("requirements", {}).get("component", {})
        Requirements(value=component_requirements) # To validate the object
        for entry in component_requirements:
            component_reference = ComponentReference(name=entry["name"])
            referenced_samplecomponent = SampleComponent(sample_reference = self.sample(), component_reference = component_reference)
            referenced_samplecomponent.load()
            requirements = pandas.json_normalize(entry["requirements"], sep=".").to_dict(orient='records')[0] # Converts the line from a dict to a 2D dataframe with 1 row, then store as a dict at sheet 0
            for requirement, expected_value in requirements.items():
                if not self._has_requirement(referenced_samplecomponent.json, requirement.split("."), expected_value):
                    no_failures = False
        if no_failures:
            return True
        else:
            return False
    def get_category(self, key: str) -> Category:
        """get the category based on provided key

        Args:
            key (str): category you want to get the value of

        Returns:
            Category: A category object of the associated key, None if not found
        """
        try:
            return Category(value=self._json["categories"][key])
        except KeyError:
            return None
    def set_category(self, category: Category):
        """set the category based on a provided Category

        Args:
            category (Category): The category you want to set for the sample
        """
        self._json["categories"][category["name"]] = category.json
    def save_files(self) -> None:
        component = Component.load(self.component)
        file_paths = component.get("db_values_changes", {}).get("files",[])
        file_ids = []
        for file_path in file_paths:
            file_id = database_interface.save_file(self._json["id"], self._json["name"], self._object_type, file_path)
            if file_id is not None:
                file_ids.append({"_id": file_id, "path": file_path})
        self._json["files"] = file_ids
class RunComponentReference(BifrostObjectReference):
    """RunComponent reference object

    Args:
        BifrostObjectReference: Inherited data type
    """
    _reference_type = "run_component"

    def __init__(self, schema_version: str = "v2_1_0", value: Dict = {}, _id: str = None, name: str = None):
        """Initialization

        Args:
            schema_version (str, optional): Schema version from json schema (bifrost.jsonc). Defaults to "v2_1_0".
            value (Dict, optional): json formatted values to be added. Defaults to {}.
            _id (str, optional): Unique id, if empty name is required to load properly, if name is also provided then id is used. Defaults to None.
            name (str, optional): Unique name, if empty id is required . Defaults to None.
        """
        BifrostObjectReference.__init__(self, schema_version, value, _id, name)

    @staticmethod
    def name_generator(run_ref: RunReference, component_ref: ComponentReference):
        if run_ref["name"] is None or component_ref["name"] is None:
            return None
        else:
            return f"{run_ref['name']}___{component_ref['name']}"
class RunComponent(BifrostObject):
    """Run Component, aka results of a component on a run, is the result object

    Args:
        BifrostObject: Inherited data type
    """
    _object_type: str = "run_component"

    def __init__(self, schema_version: str = "v2_1_0", value: Dict = None, run_reference: RunReference = None, component_reference: ComponentReference = None) -> None:
        """Initializatiion

        Args:
            schema_version (str, optional): Schema version from json schema (bifrost.jsonc). Defaults to "v2_1_0".
            value (Dict, optional): json formatted values to be added. Defaults to {}.
            sample_reference (SampleReference, optional): Reference to sample. Defaults to None.
            component_reference (ComponentReference, optional): Reference to component. Defaults to None.
        """
        if value is None:
            value = {}
        if run_reference is not None:
            value.update({"run": run_reference.json})
        if component_reference is not None:
            value.update({"component": component_reference.json})
        BifrostObject.__init__(self, schema_version, value)
    @property
    def run(self) -> RunReference:
        """get run associated to the runcomponent

        Args:
            ComponentReference: run reference associated to runcomponent
        """
        return RunReference(value = self._json["run"])
    @run.setter
    def run(self, run = RunReference) -> None:
        """set run associated to the runcomponent

        Args:
            ComponentReference: run reference associated to runcomponent
        """
        self._json["run"] = run
    @property
    def component(self) -> ComponentReference:
        """get component associated to the runcomponent

        Args:
            ComponentReference: component reference associated to runcomponent
        """
        return ComponentReference(value = self._json["component"])
    @component.setter
    def component(self, component:ComponentReference) -> None:
        """set component associated to the runcomponent

        Args:
            ComponentReference: component reference associated to runcomponent
        """
        self._json["component"] = component
    def set_name(self):
        self._json["name"] = RunComponentReference.name_generator(self.run, self.component)

