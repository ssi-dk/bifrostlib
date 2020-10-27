# I'll split out all the objects to their own files
from bifrostlib import database_interface
import os
import json
import jsmin
import warlock
from typing import List, Dict

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
        with open(os.path.join(__package__,"schemas/bifrost.jsonc"), "r") as file_stream:
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

class BifrostObjectDataType():
    """For schema datatypes
    """
    def __init__(self, datatype: str, _json: Dict):
        """initialization of to create a validated json dict 

        Args:
            program_type (str): Organization va
            datatype (str): datatype name
            requirements (Dict): dict 
        """
        schema = get_schema_datatypes(datatype)
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

class BifrostObjectReference():
    def __init__(self, reference_type: str, schema_version: str, _id: str = None, name: str = None, value: Dict = {}):
        self._reference_type = reference_type
        schema = get_schema_reference(reference_type, schema_version)
        self._model = warlock.model_factory(schema)
        entry = {}
        if _id is not None:
            entry.update({"_id": _id})
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


class BifrostObject():
    def __init__(self, object_type: str, schema_version: str, value: Dict = {}, reference:BifrostObjectReference = None) -> None:
        self._object_type = object_type
        self._schema_version = schema_version
        schema = get_schema_object(self._object_type, self._schema_version)
        self._model = warlock.model_factory(schema)
        if reference is None:
            self._json = self._model(value)
        else:
            self.load(reference)
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
        self._json = self._model(json_object)
    def load_from_reference(self, reference: BifrostObjectReference) -> None:
        self.load(reference)
    def save(self) -> None:
        database_interface.save(self._object_type, self._json)
    def delete(self) -> None:
        database_interface.delete(self._object_type, self.to_reference().json)
    def to_reference(self, additional_values: Dict = {}) -> BifrostObjectReference:
        entry = {}
        if self._json.get("_id") is not None:
            entry.update({"_id": self._json["_id"]})
        if self._json.get("name") is not None:
            entry.update({"name": self._json["name"]})
        entry.update(additional_values)
        return BifrostObjectReference(reference_type = self._object_type, schema_version = self._schema_version, value = entry)

class Category(BifrostObject):
    def __init__(self, schema_version = "2.1"):
        self._object_type = "category"
        entry = {}
        BifrostObject.__init__(self, object_type = self._object_type, schema_version = schema_version, value = entry)

class Sample(BifrostObject): # Alternative name is genomicsample
    def __init__(self, name: str = None, reference:BifrostObjectReference = None, schema_version = "2.1"):
        self._object_type = "sample"
        entry = {
            "name": name, 
            "components": [], 
            "properties": {}
        }
        BifrostObject.__init__(self, object_type = self._object_type, schema_version = schema_version, value = entry, reference = reference)
    @property
    def properties(self) -> Category:
        categories = []
        for i in self._json["properties"]:
            categories.append({i: self._json["properties"][i]})
            # categories.append(Category(i, self._json["properties"][i]))
        return categories
    @properties.setter
    def properties(self, categories: List[Category]) -> None:
        self._json["properties"] = categories
    @property
    def components(self) -> List[BifrostObjectReference]:
        components = []
        for i in self._json["components"]:
            components.append(BifrostObjectReference("component", i))
        return components
    @components.setter
    def components(self, components = List[BifrostObjectReference]) -> None:
        for i in components:
            assert(i.reference_type == "component")
        self._json["components"] = components
    def get_category(self, key: str) -> Category:
        try:
            return Category(self._json["properties"][key])
        except KeyError:
            raise AttributeError(key)
    def set_category(self, category: Category):
        self._json["properties"][category["name"]] = category


class Run(BifrostObject): # Alternative name is collection
    def __init__(self, name: str = None, reference:BifrostObjectReference = None, schema_version = "2.1"):
        self._object_type = "run"
        entry = {
            "name": name, 
            "samples": [], 
            "components": [], 
            "hosts": []
        }
        BifrostObject.__init__(self, object_type = self._object_type, schema_version = schema_version, value = entry, reference = reference)
    @property
    def samples(self) -> List[BifrostObjectReference]:
        samples = []
        for i in self._json["samples"]:
            samples.append(BifrostObjectReference("sample", i))
        return samples
    @samples.setter
    def samples(self, samples: List[BifrostObjectReference]) -> None:
        for i in samples:
            assert(i.reference_type == "sample")
        self._json["samples"] = samples
    @property
    def components(self) -> List[BifrostObjectReference]:
        components = []
        for i in self._json["components"]:
            components.append(BifrostObjectReference("component", i))
        return components
    @components.setter
    def components(self, components = List[BifrostObjectReference]) -> None:
        for i in components:
            assert(i.reference_type == "component")
        self._json["components"] = components
    @property
    def hosts(self) -> List[BifrostObjectReference]:
        hosts = []
        for i in self._json["hosts"]:
            hosts.append(BifrostObjectReference("host", i))
        return hosts
    @hosts.setter
    def hosts(self, hosts = List[BifrostObjectReference]) -> None:
        for i in hosts:
            assert(i.reference_type == "host")
        self._json["hosts"] = hosts

class Component(BifrostObject): # Alternative name is pipeline
    def __init__(self, name: str = None, reference:BifrostObjectReference = None, schema_version = "2.1"):
        self._object_type = "component"
        requirements = {
            "name": name
        }
        BifrostObject.__init__(self, self._object_type, schema_version, requirements, reference)

class Host(BifrostObject): 
    def __init__(self, name: str = None, reference:BifrostObjectReference = None, schema_version = "2.1"):
        self._object_type = "host"
        entry = {
            "name": name, 
            "samples": []
        }
        BifrostObject.__init__(self, object_type = self._object_type, schema_version = schema_version, value = entry, reference = reference)
        @property
        def samples(self) -> List[BifrostObjectReference]:
            hosts = []
            for i in self._json["samples"]:
                hosts.append(BifrostObjectReference("sample", i))
            return hosts
        @samples.setter
        def samples(self, samples = List[BifrostObjectReference]) -> None:
            for i in samples:
                assert(i.reference_type == "sample")
            self._json["samples"] = samples

class SampleComponent(BifrostObject):
    def __init__(self, sample_ref:BifrostObjectReference = None, component_ref:BifrostObjectReference = None, reference:BifrostObjectReference = None, schema_version = "2.1"):
        self._object_type = "sample_component"
        entry = {
            "sample": sample_ref,
            "component": component_ref
        }
        BifrostObject.__init__(self, object_type = self._object_type, schema_version = schema_version, value = entry, reference = reference)
        @property
        def sample(self) -> BifrostObjectReference:
            return BifrostObjectReference("sample", self._json["sample"])
        @sample.setter
        def sample(self, sample = BifrostObjectReference) -> None:
            assert(sample._object_type == "sample")
            self._json["sample"] = sample
        @property
        def component(self) -> BifrostObjectReference:
            return BifrostObjectReference("component", self._json["component"])
        @component.setter
        def component(self, component = BifrostObjectReference) -> None:
            assert(component._object_type == "component")
            self._json["component"] = component
        def _has_requirement(object_json, requirement, expected_value) -> bool:
            import functools
            try:
                value = functools.reduce(dict.get, requirement, object_json)
                if expected_value is None:
                    print(f"[true] Requirement(value:{value}, expected_value: NA, requirement{requirement}")
                    return True
                elif type(expected_value) is not list:
                    expected_value = [expected_value]
                if value in expected_value:
                    print(f"[true] Requirement(value:{value}, expected_value:{expected_value}, requirement{requirement}")
                    return True
                else:
                    print(f"[fail] Requirement(value:{value}, expected_value:{expected_value}, requirement{requirement}")
                    return False
            except Exception:
                print(f"[fail] Requirement(value:<Failed to retrieve>, expected_value:{expected_value}, requirement{requirement}")
                return False
        def has_requirements(self) -> bool:
            import pandas
            component = Component(reference = self.component())
            sample = Sample(reference = self.sample())
            
            no_failures = True
            sample_requirements = component.get("requirements", {}).get("sample", {})
            requirements = pandas.json_normalize(sample_requirements, sep=".").to_dict(orient='records')[0] # Converts the line from a dict to a 2D dataframe with 1 row, then store as a dict at sheet 0
            for requirement, expected_value in requirements.items():
                if not self._has_requirement(sample.json, requirement.split("."), expected_value):
                    no_failures = False
            component_requirements = component.get("requirements", {}).get("component", {})
            BifrostObjectDataType.__init__(self, datatype="requirements", _json=component_requirements) # To validate the object
            for entry in component_requirements:
                component_ref = BifrostObjectReference(reference_type = "components", schema_version = "2.1", name = entry["name"])
                referenced_samplecomponent = SampleComponent(sample_ref = self.sample(), component_ref = component_ref)
                referenced_samplecomponent.load()
                requirements = pandas.json_normalize(entry["requirements"], sep=".").to_dict(orient='records')[0] # Converts the line from a dict to a 2D dataframe with 1 row, then store as a dict at sheet 0
                for requirement, expected_value in requirements.items():
                    if not self._has_requirement(referenced_samplecomponent.json, requirement.split("."), expected_value):
                        no_failures = False
            if no_failures:
                return True
            else:
                return False

class RunComponent(BifrostObject):
    def __init__(self, run_ref: BifrostObjectReference = None, component_ref: BifrostObjectReference = None, reference:BifrostObjectReference = None, schema_version = "2.1"):
        self._object_type = "run_component"
        entry = {
            "sample": run_ref,
            "component": component_ref
        }
        BifrostObject.__init__(self, object_type = self._object_type, schema_version = schema_version, value = entry, reference = reference)
        @property
        def run(self) -> BifrostObjectReference:
            return BifrostObjectReference("run", self._json["run"])
        @run.setter
        def run(self, run = BifrostObjectReference) -> None:
            assert(run._object_type == "run")
            self._json["run"] = run
        @property
        def component(self) -> BifrostObjectReference:
            return BifrostObjectReference("component", self._json["component"])
        @component.setter
        def component(self, component = BifrostObjectReference) -> None:
            assert(component._object_type == "component")
            self._json["component"] = component