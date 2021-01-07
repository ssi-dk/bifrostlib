import pytest
from bifrostlib import datahandling
from bifrostlib import database_interface
from bifrostlib.datahandling import Category
from bifrostlib.datahandling import ComponentReference
from bifrostlib.datahandling import Component
from bifrostlib.datahandling import SampleReference
from bifrostlib.datahandling import Sample
from bifrostlib.datahandling import HostReference
from bifrostlib.datahandling import Host
from bifrostlib.datahandling import RunReference
from bifrostlib.datahandling import Run
from bifrostlib.datahandling import SampleComponentReference
from bifrostlib.datahandling import SampleComponent
from bifrostlib.datahandling import RunComponentReference
from bifrostlib.datahandling import RunComponent
import pymongo
import os
import time


@pytest.fixture
def test_connection():
    assert datahandling.has_a_database_connection()
    assert "TEST" in os.environ['BIFROST_DB_KEY'].upper()  # A very basic piece of protection ensuring the word test is in the DB


def test_load_schema():
    schema = datahandling.load_schema()
    assert schema is not None

class TestComponents:
    json_entries = [{"_id": {"$oid": "000000000000000000000001"}, "name": "test_component1"}]
    bson_entries = [database_interface.json_to_bson(i) for i in json_entries]

    @classmethod
    def setup_class(cls):
        client = pymongo.MongoClient(os.environ['BIFROST_DB_KEY'])
        db = client.get_database()
        cls.clear_all_collections(db)
        col = db["components"]
        col.insert_many(cls.bson_entries)

    @classmethod
    def teardown_class(cls):
        client = pymongo.MongoClient(os.environ['BIFROST_DB_KEY'])
        db = client.get_database()
        cls.clear_all_collections(db)

    @staticmethod
    def clear_all_collections(db):
        db.drop_collection("components")
        db.drop_collection("hosts")
        db.drop_collection("run_components")
        db.drop_collection("runs")
        db.drop_collection("sample_components")
        db.drop_collection("samples")

    def test_component_create(self):
        test_component = Component(name="test_component")
        print(test_component)
        test_component.save()
        assert "_id" in test_component.json

    def test_component_create_from_ref(self):
        _id = "000000000000000000000001"
        name = "test_component"
        component = Component.load(reference=ComponentReference(_id=_id, name=name))
        assert component.delete() == True
        test_component = Component(value=self.json_entries[0])
        test_component.save()
        json = component.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]

    def test_component_load(self):
        _id = "000000000000000000000001"
        name = "test_component1"
        # Test load on just _id
        reference = ComponentReference(_id=_id)
        component = Component.load(reference)
        json = component.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del component
        # Test load on just name
        refrence = ComponentReference(name=name)
        component = Component.load(reference)
        json = component.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del component
        # Test load on both _id and name
        reference = ComponentReference(_id=_id, name=name)
        component = Component.load(reference)
        json = component.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del component
        # Test load on both _id and name
        reference = ComponentReference(value=self.json_entries[0])
        component = Component.load(reference)
        json = component.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del component

    def test_component_delete(self):
        _id = "000000000000000000000001"
        name = "test_component"
        component = Component.load(ComponentReference(_id=_id, name=name))
        assert component.delete() == True


class TestSamples:
    json_entries = [{"_id": {"$oid": "000000000000000000000001"}, "name": "test_sample1", "components": [], "categories": {}}]
    bson_entries = [database_interface.json_to_bson(i) for i in json_entries]

    @classmethod
    def setup_class(cls):
        client = pymongo.MongoClient(os.environ['BIFROST_DB_KEY'])
        db = client.get_database()
        cls.clear_all_collections(db)
        col = db["samples"]
        col.insert_many(cls.bson_entries)

    @classmethod
    def teardown_class(cls):
        client = pymongo.MongoClient(os.environ['BIFROST_DB_KEY'])
        db = client.get_database()
        cls.clear_all_collections(db)

    @staticmethod
    def clear_all_collections(db):
        db.drop_collection("components")
        db.drop_collection("hosts")
        db.drop_collection("run_components")
        db.drop_collection("runs")
        db.drop_collection("sample_components")
        db.drop_collection("samples")

    def test_sample_create(self):
        test_sample = Sample(name="test_sample")
        test_sample.save()
        assert "_id" in test_sample.json

    def test_sample_create_from_ref(self):
        _id = "000000000000000000000001"
        name = "test_sample"
        sample = Sample.load(SampleReference(_id=_id, name=name))
        assert sample.delete() == True
        test_sample = Sample(value=self.json_entries[0])
        test_sample.save()
        json = sample.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]

    def test_sample_load(self):
        _id = "000000000000000000000001"
        name = "test_sample1"
        # Test load on just _id
        reference = SampleReference(_id=_id)
        sample = Sample.load(reference)
        json = sample.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del sample
        # Test load on just name
        refrence = SampleReference(name=name)
        sample = Sample.load(reference)
        json = sample.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del sample
        # Test load on both _id and name
        reference = SampleReference(_id=_id, name=name)
        sample = Sample.load(reference)
        json = sample.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del sample
        # Test load on both _id and name
        reference = SampleReference(value=self.json_entries[0])
        sample = Sample.load(reference)
        json = sample.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del sample

    def test_sample_delete(self):
        _id = "000000000000000000000001"
        name = "test_sample"
        sample = Sample.load(SampleReference(_id=_id, name=name))
        assert sample.delete() == True


class TestHosts:
    json_entries = [{"_id": {"$oid": "000000000000000000000001"}, "name": "test_host1", "samples": []}]
    bson_entries = [database_interface.json_to_bson(i) for i in json_entries]

    @classmethod
    def setup_class(cls):
        client = pymongo.MongoClient(os.environ['BIFROST_DB_KEY'])
        db = client.get_database()
        cls.clear_all_collections(db)
        col = db["hosts"]
        col.insert_many(cls.bson_entries)

    @classmethod
    def teardown_class(cls):
        client = pymongo.MongoClient(os.environ['BIFROST_DB_KEY'])
        db = client.get_database()
        cls.clear_all_collections(db)

    @staticmethod
    def clear_all_collections(db):
        db.drop_collection("components")
        db.drop_collection("hosts")
        db.drop_collection("run_components")
        db.drop_collection("runs")
        db.drop_collection("sample_components")
        db.drop_collection("samples")

    def test_host_create(self):
        test_host = Host(name="test_host")
        test_host.save()
        assert "_id" in test_host.json

    def test_host_create_from_ref(self):
        _id = "000000000000000000000001"
        name = "test_host"
        host = Host.load(HostReference(_id=_id, name=name))
        assert host.delete() == True
        test_host = Host(value=self.json_entries[0])
        test_host.save()
        json = host.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]

    def test_host_load(self):
        _id = "000000000000000000000001"
        name = "test_host1"
        # Test load on just _id
        reference = HostReference(_id=_id)
        host = Host.load(reference)
        json = host.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del host
        # Test load on just name
        refrence = HostReference(name=name)
        host = Host.load(reference)
        json = host.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del host
        # Test load on both _id and name
        reference = HostReference(_id=_id, name=name)
        host = Host.load(reference)
        json = host.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del host
        # Test load on both _id and name
        reference = HostReference(value=self.json_entries[0])
        host = Host.load(reference)
        json = host.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del host

    def test_host_delete(self):
        _id = "000000000000000000000001"
        name = "test_host"
        host = Host.load(HostReference(_id=_id, name=name))
        assert host.delete() == True


class TestRuns:
    json_entries = [{"_id": {"$oid": "000000000000000000000001"}, "name": "test_run1", "samples": [], "components": [], "hosts":[]}]
    bson_entries = [database_interface.json_to_bson(i) for i in json_entries]

    @classmethod
    def setup_class(cls):
        client = pymongo.MongoClient(os.environ['BIFROST_DB_KEY'])
        db = client.get_database()
        cls.clear_all_collections(db)
        col = db["runs"]
        col.insert_many(cls.bson_entries)

    @classmethod
    def teardown_class(cls):
        client = pymongo.MongoClient(os.environ['BIFROST_DB_KEY'])
        db = client.get_database()
        cls.clear_all_collections(db)

    @staticmethod
    def clear_all_collections(db):
        db.drop_collection("components")
        db.drop_collection("hosts")
        db.drop_collection("run_components")
        db.drop_collection("runs")
        db.drop_collection("sample_components")
        db.drop_collection("samples")

    def test_run_create(self):
        test_run = Run(name="test_run")
        test_run.save()
        assert "_id" in test_run.json

    def test_run_create_from_ref(self):
        _id = "000000000000000000000001"
        name = "test_run"
        run = Run.load(RunReference(_id=_id, name=name))
        assert run.delete() == True
        test_run = Run(value=self.json_entries[0])
        test_run.save()
        json = run.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]

    def test_run_load(self):
        _id = "000000000000000000000001"
        name = "test_run1"
        # Test load on just _id
        reference = RunReference(_id=_id)
        run = Run.load(reference)
        json = run.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del run
        # Test load on just name
        refrence = RunReference(name=name)
        run = Run.load(reference)
        json = run.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del run
        # Test load on both _id and name
        reference = RunReference(_id=_id, name=name)
        run = Run.load(reference)
        json = run.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del run
        # Test load on both _id and name
        reference = RunReference(value=self.json_entries[0])
        run = Run.load(reference)
        json = run.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del run

    def test_run_delete(self):
        _id = "000000000000000000000001"
        name = "test_run"
        run = Run.load(RunReference(_id=_id, name=name))
        assert run.delete() == True


class TestSampleComponents:
    json_entries_samples = [{"_id": {"$oid": "0000000000000000000000a1"}, "name": "test_sample1", "components": [], "categories": {}}]
    bson_entries_samples = [database_interface.json_to_bson(i) for i in json_entries_samples]
    json_entries_components = [{"_id": {"$oid": "0000000000000000000000b1"}, "name": "test_component1"}]
    bson_entries_components = [database_interface.json_to_bson(i) for i in json_entries_components]
    json_entries = [{"_id": {"$oid": "000000000000000000000001"}, "name": "test_sample_component1", "sample": {"_id": {"$oid": "0000000000000000000000a1"}, "name": "test_sample1"}, "component": {"_id": {"$oid": "0000000000000000000000b1"}, "name": "test_component1"}}]
    bson_entries = [database_interface.json_to_bson(i) for i in json_entries]

    @classmethod
    def setup_class(cls):
        client = pymongo.MongoClient(os.environ['BIFROST_DB_KEY'])
        db = client.get_database()
        cls.clear_all_collections(db)
        col = db["samples"]
        col.insert_many(cls.bson_entries_samples)
        col = db["components"]
        col.insert_many(cls.bson_entries_components)
        col = db["sample_components"]
        col.insert_many(cls.bson_entries)

    @classmethod
    def teardown_class(cls):
        client = pymongo.MongoClient(os.environ['BIFROST_DB_KEY'])
        db = client.get_database()
        cls.clear_all_collections(db)

    @staticmethod
    def clear_all_collections(db):
        db.drop_collection("components")
        db.drop_collection("hosts")
        db.drop_collection("run_components")
        db.drop_collection("runs")
        db.drop_collection("sample_components")
        db.drop_collection("samples")

    def test_sample_component_create(self):
        sample = Sample(value=self.json_entries_samples[0])
        component = Component(value=self.json_entries_components[0])
        test_sample_component = SampleComponent(sample_reference=sample.to_reference(), component_reference=component.to_reference())
        test_sample_component.save()
        assert "_id" in test_sample_component.json

    def test_sample_component_load(self):
        _id = "000000000000000000000001"
        name = "test_sample_component1"
        # Test load on just _id
        reference = SampleComponentReference(_id=_id)
        sample_component = SampleComponent.load(reference)
        json = sample_component.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del sample_component
        # Test load on just name
        refrence = SampleComponentReference(name=name)
        sample_component = SampleComponent.load(reference)
        json = sample_component.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del sample_component
        # Test load on both _id and name
        reference = SampleComponentReference(_id=_id, name=name)
        sample_component = SampleComponent.load(reference)
        json = sample_component.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del sample_component
        # Test load on both _id and name
        reference = SampleComponentReference(value=self.json_entries[0])
        sample_component = SampleComponent.load(reference)
        json = sample_component.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del sample_component

    def test_sample_component_delete(self):
        _id = "000000000000000000000001"
        name = "test_sample_component"
        sample_component = SampleComponent.load(SampleComponentReference(_id=_id, name=name))
        assert sample_component.delete() == True


class TestRunComponents:
    json_entries_runs = [{"_id": {"$oid": "000000000000000000000001"}, "name": "test_run1", "samples": [], "components": [], "hosts":[]}]
    bson_entries_runs = [database_interface.json_to_bson(i) for i in json_entries_runs]
    json_entries_components = [{"_id": {"$oid": "0000000000000000000000b1"}, "name": "test_component1"}]
    bson_entries_components = [database_interface.json_to_bson(i) for i in json_entries_components]
    json_entries = [{"_id": {"$oid": "000000000000000000000001"}, "name": "test_sample_component1", "sample": {"_id": {"$oid": "0000000000000000000000a1"}, "name": "test_sample1"}, "component": {"_id": {"$oid": "0000000000000000000000b1"}, "name": "test_component1"}}]
    bson_entries = [database_interface.json_to_bson(i) for i in json_entries]

    @classmethod
    def setup_class(cls):
        client = pymongo.MongoClient(os.environ['BIFROST_DB_KEY'])
        db = client.get_database()
        cls.clear_all_collections(db)
        col = db["runs"]
        col.insert_many(cls.bson_entries_runs)
        col = db["components"]
        col.insert_many(cls.bson_entries_components)
        col = db["run_components"]
        col.insert_many(cls.bson_entries)

    @classmethod
    def teardown_class(cls):
        client = pymongo.MongoClient(os.environ['BIFROST_DB_KEY'])
        db = client.get_database()
        cls.clear_all_collections(db)

    @staticmethod
    def clear_all_collections(db):
        db.drop_collection("components")
        db.drop_collection("hosts")
        db.drop_collection("run_components")
        db.drop_collection("runs")
        db.drop_collection("sample_components")
        db.drop_collection("samples")

    def test_run_component_create(self):
        run = Run(value=self.json_entries_runs[0])
        component = Component(value=self.json_entries_components[0])
        test_run_component = RunComponent(run_reference=run.to_reference(), component_reference=component.to_reference())
        test_run_component.save()
        assert "_id" in test_run_component.json

    def test_run_component_load(self):
        _id = "000000000000000000000001"
        name = "test_run_component1"
        # Test load on just _id
        reference = RunComponentReference(_id=_id)
        run_component = RunComponent.load(reference)
        json = run_component.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del run_component
        # Test load on just name
        refrence = RunComponentReference(name=name)
        run_component = RunComponent.load(reference)
        json = run_component.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del run_component
        # Test load on both _id and name
        reference = RunComponentReference(_id=_id, name=name)
        run_component = RunComponent.load(reference)
        json = run_component.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del run_component
        # Test load on both _id and name
        reference = RunComponentReference(value=self.json_entries[0])
        run_component = RunComponent.load(reference)
        json = run_component.json
        json.pop("version", None)
        json.pop("metadata", None)
        assert json == self.json_entries[0]
        del run_component

    def test_run_component_delete(self):
        _id = "000000000000000000000001"
        name = "test_run_component"
        run_component = RunComponent.load(RunComponentReference(_id=_id, name=name))
        assert run_component.delete() == True
