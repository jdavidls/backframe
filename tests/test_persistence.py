import unittest

from pydantic import TypeAdapter
from rich import print

from backframe.persistence import Persistence, Persistent, repository_of


class MyPersisted(Persistent): ...


class IntegrationTest(unittest.TestCase):

    def test_schemas(self):
        id_adapter = TypeAdapter(Persistent.Id)
        id = Persistent.Id.new()
        id_json = f'"{str(id)}"'.encode()

        assert id == id_adapter.dump_python(id)
        assert id == id_adapter.validate_python(id)

        assert id_json == id_adapter.dump_json(id)
        assert id == id_adapter.validate_json(id_json)

        ref_adapter = TypeAdapter(MyPersisted.Ref)
        ref = MyPersisted.Ref.from_id(id)

        assert id == ref_adapter.dump_python(ref)
        assert ref == ref_adapter.validate_python(id)

        assert id_json == ref_adapter.dump_json(ref)
        assert ref == ref_adapter.validate_json(id_json)


class PersistenceTest(unittest.IsolatedAsyncioTestCase):

    async def test_crud(self):
        async with Persistence.default().session():
            # Create a new object
            created = MyPersisted()
            assert created.id is None

            # insert the object
            await created.store()
            assert created.id is not None

            # fetch the object
            fetched = await created.ref.fetch()

            assert created == fetched
            assert created.id == fetched.id

            # update de object
            await fetched.store()

            # delete de object
            await fetched.ref.delete()

    async def test_find(self):
        async with Persistence.default().session():
            ref = await MyPersisted().store()

            async for item in repository_of(MyPersisted).find():
                print(item)

            await ref.delete()
