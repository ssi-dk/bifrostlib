import os
import datetime
import ruamel.yaml
import pandas
import functools
from bifrostlib import database_interface
import pymongo
import traceback
import sys
import subprocess
import json
import jsmin
import warlock
import bson
from typing import List, Set, Dict, Tuple, Optional


# ObjectId.yaml_tag = u'!bson.objectid.ObjectId'
# ObjectId.to_yaml = classmethod(
#     lambda cls, representer, node: representer.represent_scalar(cls.yaml_tag, u'{}'.format(node)))
# ObjectId.from_yaml = classmethod(
#     lambda cls, constructor, node: cls(node.value))

# Int64.yaml_tag = u'!bson.int64.Int64'
# Int64.to_yaml = classmethod(
#     lambda cls, representer, node: representer.represent_scalar(cls.yaml_tag, u'{}'.format(node)))
# Int64.from_yaml = classmethod(
#     lambda cls, constructor, node: cls(node.value))

# yaml = ruamel.yaml.YAML(typ="safe")
# yaml.default_flow_style = False
# yaml.register_class(ObjectId)
# yaml.register_class(Int64)

global BIFROST_SCHEMA
BIFROST_SCHEMA = None

def load_schema() -> Dict:
    global BIFROST_SCHEMA
    if BIFROST_SCHEMA == None:
        with open("schemas/bifrost.jsonc", "r") as file_stream:
            minified_bifrost_schema = jsmin.jsmin(file_stream.read())
        BIFROST_SCHEMA = json.loads(minified_bifrost_schema)
    return BIFROST_SCHEMA

def get_schema_object(object_type: str, schema_version: str) -> Dict:
    BIFROST_SCHEMA = load_schema()
    object_schema = BIFROST_SCHEMA.get("definitions",{}).get("objects",{}).get(object_type,{}).get(schema_version,{})
    object_schema["definitions"] = {}
    object_schema["definitions"]["datatypes"] = BIFROST_SCHEMA.get("definitions",{}).get("datatypes",{})
    object_schema["definitions"]["references"] = BIFROST_SCHEMA.get("definitions",{}).get("references",{})
    return object_schema

def get_schema_datatypes(reference_type: str) -> Dict:
    BIFROST_SCHEMA = load_schema()
    return BIFROST_SCHEMA.get("definitions",{}).get("datatypes",{}).get("mongoDB", {}).get(reference_type, {})

def get_schema_reference(reference_type: str, schema_version: str) -> Dict:
    BIFROST_SCHEMA = load_schema()
    return BIFROST_SCHEMA.get("definitions",{}).get("references",{}).get(reference_type, {})

class ObjectID():
    def __init__(self, id: str):
        schema = get_schema_datatypes("objectId")
        self._model = warlock.model_factory(schema)
        self._json = self._model({"$oid": id})

class BifrostObjectReference():
    def __init__(self, reference_type: str, requirements: Dict, schema_version: str):
        schema = get_schema_reference(reference_type, schema_version)
        self._model = warlock.model_factory(schema)
        self._json = self._model(requirements)

class BifrostObject():
    def __init__(self, object_type: str, required: Dict, schema_version: str) -> None:
        self._object_type = object_type
        self._schema_version = schema_version
        schema = get_schema_object(self._object_type, self._schema_version)
        self._model = warlock.model_factory(schema)
        self._json = self._model(required)
    def __eq__(self, json_dict: Dict) -> None:
        self._json = self._model(json_dict)
    def load(self, id: ObjectID) -> None:
        # NOTE: If you attempt to load you're attempting to load on the specific schema only
        json_object: Dict = database_interface.load(self._object_type, id)
        self._json = self._model(json_object)
    def save(self) -> None:
        database_interface.save(self._object_type, self._json)
    def delete(self) -> None:
        id: ObjectID = self.get_value_at("_id")
        database_interface.delete(self._object_type, id)
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
    def components(self) -> List[ComponentRef]:
        components = []
        for i in self.get_value_at("components"):
            components.append(ComponentRef(i))
    @property.setter
    def components(self, components = List[ComponentRef]) -> None:
        self.set_value_at(["components"]) = components

class Run(BifrostObject): # Alternative name is collection
    def __init__(self, name: str = None, schema_version=2.1):
        self._object_type = "run"
        requirements = {"name": name}
        BifrostObject.__init__(self, self._object_type, requirements, schema_version)
    @property
    def samples(self) -> List[SampleRef]:
        samples = []
        for i in self.get_value_at(["samples"]):
            samples.append(SampleRef(i))
        return samples
    @samples.setter
    def samples(self, samples: List[SampleRef]) -> None:
        self.set_value_at(["samples"]) = samples
    @property
    def components(self) -> List[ComponentRef]:
        components = []
        for i in self.get_value_at("components"):
            components.append(ComponentRef(i))
        return components
    @property.setter
    def components(self, components = List[ComponentRef]) -> None:
        self.set_value_at(["components"]) = components
    @property
    def hosts(self) -> List[HostRef]:
        hosts = []
        for i in self.get_value_at("hosts"):
            hosts.append(HostRef(i))
        return hosts
    @property.setter
    def hosts(self, hosts = List[HostRef]) -> None:
        self.set_value_at(["hosts"]) = hosts
class Component(BifrostObject): # Alternative name is pipeline
    def __init__(self, name: str = None, schema_version=2.1):
        BifrostObject.__init__(self, "component", {name: name}, schema_version)

