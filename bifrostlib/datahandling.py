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
from typing import List, Dict

global BIFROST_SCHEMA
BIFROST_SCHEMA = None


def date_now() -> datetime.datetime:
    """Get the current time as a datetime

    Note: 
        Needed to keep the same date in python and mongo, as mongo rounds to millisecond

    Returns:
        Current time rounded to miliseconds
    """
    current_time = datetime.datetime.utcnow()
    current_time = current_time.replace(microsecond=math.floor(current_time.microsecond/1000)*1000)
    current_time_in_json = {"$date": str(current_time)[:-3].replace(" ","T")}
    return current_time_in_json

def has_a_database_connection():
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
        schema_version (str): the version of the object you want to work with

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
        reference_type (str): [description]
        schema_version (str): [description]

    Returns:
        Dict: [description]
    """
    BIFROST_SCHEMA = load_schema()
    object_schema = BIFROST_SCHEMA.get("definitions", {}).get("references", {}).get(reference_type, {}).get(schema_version, {})
    object_schema["definitions"] = {}
    object_schema["definitions"]["datatypes"] = BIFROST_SCHEMA.get("definitions", {}).get("datatypes", {})
    return object_schema


class BifrostObjectDataType(Dict):
    """For schema datatypes
    """
    def __init__(self, datatype: str, _json: Dict):
        """initialization of to create a validated json dict 

        Args:
            program_type (str): Organization va
            datatype (str): datatype name
            requirements (Dict): dict 
        """
        schema: Dict = get_schema_datatypes(datatype)
        self._model = warlock.model_factory(schema)
        self._json = {}
        self._json = self._model(_json)
    def __repr__(self):
        return str(self._json)
    def __getitem__(self, key):
        return self._json[key]
    def __setitem__(self, key, value):
        self._json[key] = value
    def __delitem__(self, key):
        self._json.__delitem__(key)
    @property
    def json(self):
        return self._json.copy()

class Version(BifrostObjectDataType):
    def __init__(self, datatype: str="version", code_version: str=None, resource_version: str=None, schema_version: str="v2_1_0", _json: Dict=None, ):
        if _json is None:
            _json = {
                "schema": [schema_version]
            }
            if code_version is not None:
                _json.update({"code": code_version})
            if resource_version is not None:
                _json.update({"resources": resource_version})
        BifrostObjectDataType.__init__(self, datatype=datatype, _json=_json)
    def add_schema_version(self, schema_version):
        if schema_version not in self._json["schema"]:
            self._json["schema"].append(schema_version)

class Metadata(BifrostObjectDataType):
    def __init__(self, datatype: str="metadata", _json: Dict=None, ):
        if _json is None:
            _json = {
                "created_at": date_now(),
                "updated_at": date_now()
            }
        BifrostObjectDataType.__init__(self, datatype=datatype, _json=_json)
    def updated_now(self):
        self._json["updated_at"] = date_now()


class BifrostObjectReference(Dict):
    def __init__(self, reference_type: str, schema_version: str, _id: str = None, name: str = None, value: Dict = {}):
        self._reference_type: str = reference_type
        schema: Dict = get_schema_reference(reference_type, schema_version)
        self._model = warlock.model_factory(schema)
        entry = {"name": ""}
        if _id is not None:
            entry.update({"_id": {"$oid":_id}}) # expects id as a string converts to json
        if name is not None:
            entry.update({"name": name})
        entry.update(value)
        self._json = self._model(entry)
    def __repr__(self):
        return str(self._json)
    def __getitem__(self, key):
        return self._json[key]
    def __setitem__(self, key, value):
        self._json[key] = value
    def __delitem__(self, key):
        self._json.__delitem__(key)
    @property
    def json(self):
        return self._json.copy()
    @property
    def reference_type(self):
        return self._reference_type


class BifrostObject(Dict):
    def __init__(self, object_type: str, schema_version: str, value: Dict = {}, reference:BifrostObjectReference = None) -> None:
        self._object_type: str = object_type
        self._schema_version: str = schema_version
        schema: Dict = get_schema_object(self._object_type, self._schema_version)
        self._model = warlock.model_factory(schema)
        self._json = {}
        if reference is None:
            self._json = self._model(value)
            if "metadata" not in self._json:
                self._json["metadata"] = Metadata().json
            if "version" not in self._json:
                self._json["version"] = Version(schema_version=schema_version).json
        else:
            try:
                self.load(reference)
            except Exception as e:
                raise
    def __repr__(self):
        return str(self._json)
    def __getitem__(self, key):
        return self._json[key]
    def __setitem__(self, key, value):
        self._json[key] = value
    def __delitem__(self, key):
        self._json.__delitem__(key)
    @property
    def json(self):
        return self._json.copy()
    @json.setter
    def json(self, value: Dict):
        self._json = self._model(value)
    def load(self, reference: BifrostObjectReference) -> None:
        # NOTE: If you attempt to load you're attempting to load on the specific schema only
        json_object: Dict = database_interface.load(self._object_type, reference.json)
        if json_object == {}:
            print(f"{reference.json} not found in db")
        self._json = self._model(json_object)
    def load_from_reference(self, reference: BifrostObjectReference) -> None:
        self.load(reference)
    def save(self) -> None:
        metadata = Metadata(_json=self.json["metadata"])
        metadata.updated_now()
        self._json["metadata"] = metadata.json
        self._json = self._model(database_interface.save(self._object_type, self.json))
    def delete(self) -> None:
        return database_interface.delete(self._object_type, self.to_reference().json)
    def to_reference(self, additional_values: Dict = {}) -> BifrostObjectReference:
        entry = {}
        if self._json.get("_id") is not None:
            entry.update({"_id": self._json["_id"]})
        if self._json.get("name") is not None:
            entry.update({"name": self._json["name"]})
        entry.update(additional_values)
        return BifrostObjectReference(reference_type = self._object_type, schema_version = self._schema_version, value = entry)

class Category(BifrostObject):
    def __init__(self, value = None, schema_version = "v2_1_0"):
        self._object_type = "category"
        self.schema_version = schema_version
        if value is None:
            value = {}
        BifrostObject.__init__(self, object_type = self._object_type, schema_version = schema_version, value = value)


class ComponentReference(BifrostObjectReference):
    def __init__(self, _id: str = None, name: str = None, value: Dict = {}, schema_version="v2_1_0"):
        BifrostObjectReference.__init__(self, reference_type="component", schema_version=schema_version, _id=_id, name=name, value=value)


class Component(BifrostObject):  # Alternative name is pipeline
    def __init__(self, name: str = None, value = None, reference: ComponentReference = None, schema_version="v2_1_0"):
        self._object_type = "component"
        if value is None:
            value = {
                "name": name
            }
        BifrostObject.__init__(self, object_type=self._object_type, schema_version=schema_version, value=value, reference=reference)


class SampleReference(BifrostObjectReference):
    def __init__(self, _id:str = None, name:str = None, value:Dict = {}, schema_version="v2_1_0"):
        BifrostObjectReference.__init__(self, reference_type="sample", schema_version=schema_version, _id = _id, name = name, value = value)

class Sample(BifrostObject): # Alternative name is genomicsample
    def __init__(self, name: str = None, value = None, reference:BifrostObjectReference = None, schema_version = "v2_1_0"):
        self._object_type = "sample"
        if value is None:
            value = {
                "name": name, 
                "components": [], 
                "categories": {}
            }
        BifrostObject.__init__(self, object_type = self._object_type, schema_version = schema_version, value = value, reference = reference)
    @property
    def components(self) -> List[ComponentReference]:
        components = []
        for i in self._json["components"]:
            components.append(ComponentReference(value = i))
        return components
    @components.setter
    def components(self, components = List[ComponentReference]) -> None:
        json_items = []
        for i in components:
            json_items.append(i.json)
        self._json["components"] = json_items
    def get_category(self, key: str) -> Category:
        try:
            return Category(value=self._json["categories"][key])
        except KeyError:
            raise AttributeError(key)
    def set_category(self, category: Category):
        for name in category.json:
            self._json["categories"][name] = category.json[name]

class HostReference(BifrostObjectReference):
    def __init__(self, _id: str = None, name: str = None, value: Dict = {}, schema_version="v2_1_0"):
        BifrostObjectReference.__init__(self, reference_type="host", schema_version=schema_version, _id=_id, name=name, value=value)

class Host(BifrostObject):
    def __init__(self, name: str = None, value= None, reference: HostReference = None, schema_version="v2_1_0"):
        self._object_type = "host"
        if value is None:
            value = {
                "name": name,
                "samples": []
            }
        BifrostObject.__init__(self, object_type=self._object_type, schema_version=schema_version, value=value, reference=reference)
        @property
        def samples(self) -> List[SampleReference]:
            hosts = []
            for i in self._json["samples"]:
                hosts.append(SampleReference(value=i))
            return hosts
        @samples.setter
        def samples(self, samples=List[SampleReference]) -> None:
            json_items = []
            for i in samples:
                json_items.append(i.json)
            self._json["samples"] = json_items

class RunReference(BifrostObjectReference):
    def __init__(self, _id:str = None, name:str = None, value:Dict = {}, schema_version="v2_1_0"):
        BifrostObjectReference.__init__(self, reference_type="run", schema_version=schema_version, _id = _id, name = name, value = value)

class Run(BifrostObject): # Alternative name is collection
    def __init__(self, name: str = None, value = None, reference:BifrostObjectReference = None, schema_version = "v2_1_0"):
        self._object_type = "run"
        if value is None:
            value = {
                "name": name, 
                "samples": [], 
                "components": [], 
                "hosts": []
            }
        BifrostObject.__init__(self, object_type = self._object_type, schema_version = schema_version, value = value, reference = reference)
    @property
    def samples(self) -> List[SampleReference]:
        samples = []
        for i in self._json["samples"]:
            samples.append(SampleReference(value=i))
        return samples
    @samples.setter
    def samples(self, samples: List[SampleReference]) -> None:
        json_items = []
        for i in samples:
            json_items.append(i.json)
        self._json["samples"] = json_items
    @property
    def components(self) -> List[ComponentReference]:
        components = []
        for i in self._json["components"]:
            components.append(ComponentReference(value=i))
        return components
    @components.setter
    def components(self, components=List[ComponentReference]) -> None:
        json_items = []
        for i in components:
            json_items.append(i.json)
        self._json["components"] = json_items
    @property
    def hosts(self) -> List[HostReference]:
        hosts = []
        for i in self._json["hosts"]:
            hosts.append(HostReference(value=i))
        return hosts
    @hosts.setter
    def hosts(self, hosts=List[HostReference]) -> None:
        json_items = []
        for i in hosts:
            json_items.append(i.json)
        self._json["hosts"] = json_items


class SampleComponentReference(BifrostObjectReference):
    def __init__(self, _id: str = None, name: str = None, value: Dict = {}, schema_version="v2_1_0"):
        BifrostObjectReference.__init__(self, reference_type="sample_component", schema_version=schema_version, _id=_id, name=name, value=value)

class SampleComponent(BifrostObject):
    def __init__(self, sample_reference:SampleReference = None, component_reference:ComponentReference = None, value = None, reference:SampleComponentReference = None, schema_version = "v2_1_0"):
        self._object_type = "sample_component"
        if value is None:
            value = {}
            if sample_reference is not None:
                value.update({"sample": sample_reference.json})
            if component_reference is not None:
                value.update({"component": component_reference.json})
        BifrostObject.__init__(self, object_type = self._object_type, schema_version = schema_version, value = value, reference = reference)
    @property
    def sample(self) -> SampleReference:
        return SampleReference(value=self._json["sample"])
    @sample.setter
    def sample(self, sample=SampleReference) -> None:
        self._json["sample"] = sample.json
    @property
    def component(self) -> ComponentReference:
        return ComponentReference(value=self._json["component"])
    @component.setter
    def component(self, component=ComponentReference) -> None:
        self._json["component"] = component.json
    def _has_requirement(self, object_json, requirement, expected_value) -> bool:
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
        component = Component(reference = self.component)
        sample = Sample(reference = self.sample)

        no_failures = True
        sample_requirements = component.json.get("requirements", {}).get("sample", {})
        requirements = pandas.json_normalize(sample_requirements, sep=".").to_dict(orient='records')[0] # Converts the line from a dict to a 2D dataframe with 1 row, then store as a dict at sheet 0
        
        for requirement, expected_value in requirements.items():
            if not self._has_requirement(sample.json, requirement.split("."), expected_value):
                no_failures = False
        component_requirements = component.get("requirements", {}).get("component", {})
        BifrostObjectDataType.__init__(self, datatype="requirements", _json=component_requirements) # To validate the object
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


class RunComponentReference(BifrostObjectReference):
    def __init__(self, _id: str = None, name: str = None, value: Dict = {}, schema_version="v2_1_0"):
        BifrostObjectReference.__init__(self, reference_type="run_component", schema_version=schema_version, _id=_id, name=name, value=value)

class RunComponent(BifrostObject):
    def __init__(self, run_reference: RunReference = None, component_reference: ComponentReference = None, value = None, reference:RunComponentReference = None, schema_version = "v2_1_0"):
        self._object_type = "run_component"
        if value is None:
            value = {}
            if run_reference is not None:
                value.update({"run": run_reference.json})
            if component_reference is not None:
                value.update({"component": component_reference.json})
        BifrostObject.__init__(self, object_type = self._object_type, schema_version = schema_version, value = value, reference = reference)
        @property
        def run(self) -> RunReference:
            return RunReference(value = self._json["run"])
        @run.setter
        def run(self, run = RunReference) -> None:
            self._json["run"] = run
        @property
        def component(self) -> ComponentReference:
            return ComponentReference(value = self._json["component"])
        @component.setter
        def component(self, component=ComponentReference) -> None:
            self._json["component"] = component
