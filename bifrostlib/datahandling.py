from bifrostlib import database_interface
import json
import jsmin
import warlock
from typing import List, Set, Dict, Tuple, Optional

global BIFROST_SCHEMA
BIFROST_SCHEMA = None

def load_schema() -> Dict:
    """loads BIFROST_SCHEMA from bifrost.jsonc which is the basis for objects

    Other Parameters:
        BIFROST_SCHEMA (dict): GLOBAL storing the BIFROST_SCHEMA

    Returns:
        Dict: json formatted schema
    """
    global BIFROST_SCHEMA
    if BIFROST_SCHEMA == None:
        with open("schemas/bifrost.jsonc", "r") as file_stream:
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
    return object_schema

def get_schema_datatypes(program_type: str, datatype: str) -> Dict:
    """Get a datatype schema from the BIFROST_SCHEMA

    Args:
        reference_type (str): reference type based on available references to objects in schema

    Other Parameters:
        BIFROST_SCHEMA (dict): GLOBAL storing the BIFROST_SCHEMA

    Returns:
        Dict: The datatype schema
    """
    BIFROST_SCHEMA = load_schema()
    return BIFROST_SCHEMA.get("definitions", {}).get("datatypes", {}).get(program_type, {}).get(datatype, {})

def get_schema_reference(reference_type: str, schema_version: str) -> Dict:
    """Get the reference schema of from the BIFROST_SCHEMA

    Args:
        reference_type (str): [description]
        schema_version (str): [description]

    Returns:
        Dict: [description]
    """
    BIFROST_SCHEMA = load_schema()
    return BIFROST_SCHEMA.get("definitions", {}).get("references", {}).get(reference_type, {}).get(schema_version, {})

class BifrostObjectDataType():
    """For schema datatypes
    """
    def __init__(self, program_type: str, datatype: str, requirements: Dict):
        """initialization of to create a validated json dict 

        Args:
            program_type (str): Organization va
            datatype (str): datatype name
            requirements (Dict): dict 
        """
        schema = get_schema_datatypes(program_type, datatype)
        self._model = warlock.model_factory(schema)
        self._json = self._model(requirements)
    @property
    def json(self):
        return self._json
    @json.setter
    def json(self, json_dict):
        self._json = self._model(json_dict)

class ObjectID(BifrostObjectDataType):
    def __init__(self, _id: str):
        requirements = {"$oid": _id}
        BifrostObjectDataType.__init__(self, program_type="mongoDB", datatype="objectID", requirements=requirements)

# TODO: Potentially do bifrost datatypes with functions for setting values then code is not tied to a schema version

class BifrostObjectReference():
    def __init__(self, reference_type: str, requirements: Dict, schema_version: str):
        self._reference_type = reference_type
        schema = get_schema_reference(reference_type, schema_version)
        self._model = warlock.model_factory(schema)
        self._json = self._model(requirements)
    @property
    def id(self):
        return ObjectID(self._json["_id"])
    @property
    def reference_type(self):
        return self._reference_type

class BifrostObject():
    def __init__(self, object_type: str, required: Dict, schema_version: str) -> None:
        self._object_type = object_type
        self._schema_version = schema_version
        schema = get_schema_object(self._object_type, self._schema_version)
        self._model = warlock.model_factory(schema)
        self._json = self._model(required)
    def __eq__(self, json_dict: Dict) -> None:
        self._json = self._model(json_dict)
    def load(self, _id: ObjectID) -> None:
        # NOTE: If you attempt to load you're attempting to load on the specific schema only
        json_object: Dict = database_interface.load(self._object_type, _id.json)
        self._json = self._model(json_object)
    def load_from_reference(self, reference: BifrostObjectReference) -> None:
        self.load(reference.id)
    def save(self) -> None:
        database_interface.save(self._object_type, self._json)
    def delete(self) -> None:
        _id: ObjectID = self.get_value_at("_id")
        database_interface.delete(self._object_type, _id.json)
    def get_reference(self, additional_requirements: Dict = {}) -> BifrostObjectReference:
        requirements = {
            "_id": self.get_value_at("_id"),
            "name": self.get_value_at("name"),
            
        }.update(additional_requirements)
        return BifrostObjectReference(self._object_type, requirements, self._schema_version)
    def get_value_at(self, structure: List) -> Dict:
        value = self._json
        for i in structure:
            value = value.get(i, {})
        return value
    def set_value_at(self, structure: List, value) -> None:
        try:
            for i in structure:
                temp = self._json[i]
            temp = value
        except:
            print("Not a valid location to store in the model")

class Sample(BifrostObject): # Alternative name is genomicsample
    def __init__(self, name: str = None, schema_version=2.1):
        self._object_type = "sample"
        requirements = {"name": name}
        BifrostObject.__init__(self, self._object_type, requirements, schema_version)
    @property
    def categories(self) -> List[Category]:
        categories = []
        for i in self.get_value_at(["properties"]["component"]):
            categories.append(Category(i))
        return categories
    @categories.setter
    def categories(self, category: Category) -> None:
        self.set_value_at(["properties", category.name]) = category
    @property
    def components(self) -> List[BifrostObjectReference]:
        components = []
        for i in self.get_value_at("components"):
            components.append(BifrostObjectReference("component", i))
    @components.setter
    def components(self, components = List[BifrostObjectReference]) -> None:
        for i in components:
            assert(i.reference_type == "component")
        self.set_value_at(["components"]) = components

class Run(BifrostObject): # Alternative name is collection
    def __init__(self, name: str = None, schema_version=2.1):
        self._object_type = "run"
        requirements = {"name": name}
        BifrostObject.__init__(self, self._object_type, requirements, schema_version)
    @property
    def samples(self) -> List[BifrostObjectReference]:
        samples = []
        for i in self.get_value_at(["samples"]):
            samples.append(BifrostObjectReference("sample", i))
        return samples
    @samples.setter
    def samples(self, samples: List[BifrostObjectReference]) -> None:
        for i in samples:
            assert(i.reference_type == "sample")
        self.set_value_at(["samples"]) = samples
    @property
    def components(self) -> List[BifrostObjectReference]:
        components = []
        for i in self.get_value_at("components"):
            components.append(BifrostObjectReference("component", i))
        return components
    @components.setter
    def components(self, components = List[BifrostObjectReference]) -> None:
        for i in components:
            assert(i.reference_type == "component")
        self.set_value_at(["components"]) = components
    @property
    def hosts(self) -> List[BifrostObjectReference]:
        hosts = []
        for i in self.get_value_at("hosts"):
            hosts.append(BifrostObjectReference("host", i))
        return hosts
    @hosts.setter
    def hosts(self, hosts = List[BifrostObjectReference]) -> None:
        for i in hosts:
            assert(i.reference_type == "host")
        self.set_value_at(["hosts"]) = hosts

class Component(BifrostObject): # Alternative name is pipeline
    def __init__(self, name: str = None, schema_version=2.1):
        self._object_type = "component"
        requirements = {"name": name}
        BifrostObject.__init__(self, self._object_type, requirements, schema_version)

class Host(BifrostObject): 
    def __init__(self, name: str = None, schema_version=2.1):
        self._object_type = "host"
        requirements = {"name": name}
        BifrostObject.__init__(self, self._object_type, requirements, schema_version)
        @property
        def samples(self) -> List[BifrostObjectReference]:
            hosts = []
            for i in self.get_value_at("samples"):
                hosts.append(BifrostObjectReference("sample", i))
            return hosts
        @samples.setter
        def samples(self, samples = List[BifrostObjectReference]) -> None:
            for i in samples:
                assert(i.reference_type == "sample")
            self.set_value_at(["samples"]) = samples

class SampleComponent(BifrostObject):
    def __init__(self, sample_ref: BifrostObjectReference, component_ref: BifrostObjectReference, schema_version=2.1):
        self._object_type = "sample_component"
        requirements = {}
        requirements = requirements.update({"sample": sample_ref})
        requirements = requirements.update({"component": component_ref})
        BifrostObject.__init__(self)
        @property
        def sample(self) -> BifrostObjectReference:
            return BifrostObjectReference("sample", self.get_value_at("sample"))
        @sample.setter
        def sample(self, sample = BifrostObjectReference) -> None:
            assert(sample._object_type == "sample")
            self.set_value_at(["sample"]) = sample
        @property
        def component(self) -> BifrostObjectReference:
            return BifrostObjectReference("component", self.get_value_at("component"))
        @component.setter
        def component(self, component = BifrostObjectReference) -> None:
            assert(component._object_type == "component")
            self.set_value_at(["component"]) = component
class RunComponent(BifrostObject):
    def __init__(self, run_ref: BifrostObjectReference, component_ref: BifrostObjectReference, schema_version=2.1):
        self._object_type = "run_component"
        requirements = {}
        requirements = requirements.update({"sample": run_ref})
        requirements = requirements.update({"component": component_ref})
        BifrostObject.__init__(self)
        @property
        def run(self) -> BifrostObjectReference:
            return BifrostObjectReference("run", self.get_value_at("run"))
        @run.setter
        def run(self, run = BifrostObjectReference) -> None:
            assert(run._object_type == "run")
            self.set_value_at(["run"]) = run
        @property
        def component(self) -> BifrostObjectReference:
            return BifrostObjectReference("component", self.get_value_at("component"))
        @component.setter
        def component(self, component = BifrostObjectReference) -> None:
            assert(component._object_type == "component")
            self.set_value_at(["component"]) = component