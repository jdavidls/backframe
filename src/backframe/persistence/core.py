from __future__ import annotations

from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import (
    Any,
    Callable,
    ClassVar,
    Generic,
    Literal,
    Mapping,
    Optional,
    Self,
    TypeVar,
)

from bson import ObjectId
from bson.errors import InvalidId
from inflector import English
from motor.core import AgnosticCursor
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorClientSession,
    AsyncIOMotorDatabase,
    AsyncIOMotorGridFSBucket,
)
from pydantic import BaseModel, Field, MongoDsn, TypeAdapter
from pydantic_core import core_schema


class PersistenceError(Exception):
    pass


class NotFoundError(PersistenceError, LookupError):
    pass


class BadCommandError(PersistenceError, LookupError):
    pass


_english_inflector = English()

_persistence_session: ContextVar[Optional[Persistence.Session]] = ContextVar(
    f"{__name__}._persistence_context",
    default=None,
)


@dataclass
class Persistence:
    """
    A class representing the persistence layer for the ctrl.
    """

    class Settings(BaseModel):
        """Persistence layer settings"""

        dsn: Optional[MongoDsn] = Field(None, title="Mongo database DSN")
        database: Optional[str] = Field("test", title="Name of the database")

    client: AsyncIOMotorClient
    database: AsyncIOMotorDatabase

    @classmethod
    @property
    def current(cls):
        return current_session().persistence

    @classmethod
    @property
    def session(cls):
        return current_session()

    @cached_property
    def gridfs(self):
        return AsyncIOMotorGridFSBucket(self.database)

    @classmethod
    def default(cls):
        """
        Create a `Persistence` instance with default settings.

        Returns:
            Persistence: The `Persistence` instance.
        """
        return cls.from_settings(Persistence.Settings())

    @classmethod
    def from_dsn_and_db(cls, dsn: str, db: str):
        """
        Create a `Persistence` instance from the given DSN and database name.

        Args:
            dsn (str): The MongoDB connection string.
            db (str): The name of the database.

        Returns:
            Persistence: The `Persistence` instance.
        """
        client = AsyncIOMotorClient(dsn)
        return cls(client=client, database=client.get_database(db))

    @classmethod
    def from_settings(cls, settings: Persistence.Settings):
        """
        Create a `Persistence` instance from the given settings.

        Args:
            settings (PersistenceSettings): The persistence settings.

        Returns:
            Persistence: The `Persistence` instance.
        """
        client = AsyncIOMotorClient(settings.dsn)
        return cls(client=client, database=client.get_database(settings.database))

    @dataclass
    class Session:
        """Persistence layer context.

        This class represents a persistence layer context, providing a session for interacting with the database.

        Attributes:
            persistence (Persistence): The persistence layer instance.
            _inner (AsyncIOMotorClientSession): The underlying MongoDB client session.
        """

        persistence: Persistence
        _inner: AsyncIOMotorClientSession

    @asynccontextmanager
    async def session(self):
        """
        Enter the persistence session context.

        Yields:
            PersistenceSession: The persistence session.
        """
        async with await self.client.start_session(causal_consistency=True) as session:
            session = self.Session(self, session)
            session_token = _persistence_session.set(session)
            try:
                yield session
            finally:
                _persistence_session.reset(session_token)


def current_session() -> Persistence.Session:
    """
    Returns the current persistence session.

    Raises:
        RuntimeError: If no persistence context has been established.
            You must perform 'async with persistence.session()' to be able to use the persistence layer functions.

    Returns:
        PersistenceSession: The current persistence session.
    """
    session = _persistence_session.get()
    if session is None:
        raise RuntimeError(
            "No persistence context has been established. "
            "You must perform 'async with persistence.session()' "
            "to be able to use the persistence layer functions."
        )
    return session


def in_transaction() -> bool:
    """
    Check if there is an active transaction.

    Returns:
        bool: True if there is an active transaction, False otherwise.
    """
    return current_session()._inner.in_transaction


## TODO Será un decorador de funcion...
@asynccontextmanager
async def transaction():
    """
    Executes a transaction within the current session.

    Raises:
        RuntimeError: If a nested transaction is detected.
    """
    ctx = current_session()

    if ctx._inner.in_transaction:
        raise RuntimeError(
            "Nested transactions are not supported. "
            "A transaction has been started within another transaction."
        )

    async with ctx._inner.start_transaction():
        yield


class Id(ObjectId):
    """
    Represents an identifier used in the persistence module.

    Inherits from `ObjectId` class.

    Attributes:
        created_at: The creation timestamp of the identifier.
        as_filter: The identifier formatted as a filter for database queries.
    """

    def __init__(self, *args):
        try:
            super().__init__(*args)
        except InvalidId as e:
            raise ValueError(*e.args) from InvalidId

    @classmethod
    def __get_pydantic_core_schema__(cls, *_):
        return core_schema.json_or_python_schema(
            # JSON
            core_schema.no_info_plain_validator_function(
                cls.from_str,
                serialization=core_schema.plain_serializer_function_ser_schema(
                    lambda self: str(self)  # as str
                ),
            ),
            # PYTHON
            core_schema.no_info_plain_validator_function(
                cls.from_oid,
                serialization=core_schema.plain_serializer_function_ser_schema(
                    lambda self: self  # as ObjectId
                ),
            ),
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, _, handler):
        return handler(core_schema.str_schema())

    @classmethod
    def new(cls):
        return cls()

    @classmethod
    def from_str(cls, val: str):
        assert isinstance(val, str), f"from_str receive {val}"
        return cls(val)

    @classmethod
    def from_oid(cls, val: Id | ObjectId | str):
        # assert isinstance(val, ObjectId), f"Id from python is {val}"
        return val if isinstance(val, cls) else cls(val)

    @property
    def created_at(self):
        return self.generation_time

    @property
    def as_filter(self):
        return {"_id": self}


class Persistent(BaseModel):
    """Base class of a persistent object.

    This class serves as the base class for persistent objects in the ctrl.
    It provides common functionality for storing and retrieving objects from a database.

    Attributes:
        id (Optional[Id]): Unique object identifier.
    """

    # __repository__: ClassVar[RepositoryBase[Self]]
    # Id: ClassVar[type[Id]]
    # Ref: ClassVar[type[Ref[Self]]]
    # Repository: ClassVar[type[Repository[Self]]]

    Id: ClassVar[type[Id]]

    Ref: ClassVar[type[RefBase[Self]]]

    Repository: ClassVar[type[RepositoryBase[Self]]]

    id: Optional[Id] = Field(None, title="Unique object Id", alias="_id")

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Make a repository class for the new persistent class

        rep_bases = tuple(
            {
                getattr(base, "Repository"): None
                for base in cls.__mro__
                if hasattr(base, "Repository")
            }.keys()
        )

        rep_cls = type(
            "Repository",
            rep_bases,
            {
                "__module__": cls.__module__,
                "__qualname__": f"{cls.__qualname__}.Repository",
                "__persistent__": cls,
            },
        )

        # Instance the repository
        repository = rep_cls(_english_inflector.tableize(cls.__qualname__))

        # Make a reference class for the new persistent class
        ref_bases = tuple(
            {
                getattr(base, "Ref"): None
                for base in cls.__mro__
                if hasattr(base, "Ref")
            }.keys()
        )

        ref_cls = type(
            "Ref",
            ref_bases,
            {
                "__module__": cls.__module__,
                "__qualname__": f"{cls.__qualname__}.Ref",
                "__persistent__": cls,
                "__repository__": repository,
            },
        )

        setattr(cls, "Repository", rep_cls)
        setattr(cls, "Ref", ref_cls)

        setattr(cls, "__repository__", repository)

    @property
    def ref(self) -> RefBase[Self]:
        """Return a reference to the object.

        Returns:
            Ref[Self]: A reference to the object.
        """
        return type(self).Ref(self.id)

    async def store(self) -> RefBase[Self]:
        """Persist the object in the database.shipment is

        This method will use the insert procedure if the object does not have
        an identifier defined (id is None) or the update(upsert) procedure if
        the object already has an identifier.

        Returns:
            RefBase[Self]: A reference to the persisted object.
        """
        if self.id:
            await repository_of(self).update_obj(self)
        else:
            await repository_of(self).insert_obj(self)

        return self.ref

    async def insert(self):
        assert self.id is None, f"{type(self)} already inserted with id '{self.id}'"
        await repository_of(self).insert_obj(self)

    async def update(self):
        assert self.id is not None, f"{type(self)} does not exists with id '{self.id}'"
        await repository_of(self).update_obj(self)


P = TypeVar("P", bound=Persistent)


class RefBase(Generic[P]):
    """Base class for the Persistent.Ref classes.

    The purpose of Persistent.Ref[P] classes is to uniquely identify a persistent object P).
    The 'Id' objects allow to identify, retrieve and operate remotely through
    their methods without the need to materialize in memory a stay of the
    object they identify.
    """

    __slots__: tuple[Literal["_id"]] = ("_id",)
    __persistent__: type[P]
    __repository__: RepositoryBase[P]
    __qualname__ = "Persistent.Ref"
    _id: Id

    @classmethod
    def __get_pydantic_core_schema__(cls, *_):
        return core_schema.json_or_python_schema(
            # JSON
            core_schema.no_info_plain_validator_function(
                cls.from_str,
                serialization=core_schema.plain_serializer_function_ser_schema(
                    lambda self: str(self._id)  # as str
                ),
            ),
            # PYTHON
            core_schema.no_info_plain_validator_function(
                cls.from_id,
                serialization=core_schema.plain_serializer_function_ser_schema(
                    lambda self: self._id  # as ObjectId
                ),
            ),
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, _, handler):
        return handler(core_schema.str_schema())

    def __repr__(self):
        return f"{type(self).__qualname__}({self._id})"

    @classmethod
    def from_str(cls, value: str) -> Self:
        assert isinstance(value, str)
        return cls(Id.from_str(value))

    @classmethod
    def from_id(cls, value: Id | ObjectId):
        if isinstance(value, RefBase):
            value = value.id
        return cls(Id.from_oid(value))

    def __init__(self, id: Id):
        if not isinstance(id, Id):
            raise TypeError(f"Bad id type {type(id)}")
        self._id = id

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, type(self)):
            return self._id == other._id
        return False

    def __hash__(self) -> int:
        return hash(self._id)

    @property
    def created_at(self) -> datetime:
        return self._id.generation_time

    @property
    def id(self):
        return self._id

    async def fetch(self) -> P:
        """
        Fetches the object with the specified ID from the repository.

        Returns:
            The fetched object.
        """
        return await repository_of(self).fetch_by_id(self._id)

    async def delete(self):
        """
        Deletes the object from the repository by its ID.

        Returns:
            A coroutine that deletes the object from the repository.
        """
        return await repository_of(self).delete_by_id(self._id)

    async def exists(self) -> bool:
        """
        Check if the object exists in the repository.

        Returns:
            bool: True if the object exists, False otherwise.
        """
        count = await repository_of(self).count(self._id.as_filter)
        return count == 1

    async def complies(self, conditions: Mapping[str, Any]) -> bool:
        """
        Check if the object complies with the given conditions.

        Args:
            conditions (Mapping[str, Any]): The conditions to check against.

        Returns:
            bool: True if the object complies with the conditions, False otherwise.

        Examples:
            #>>> await user_ref.complies({"profile.first_name": "John"})
        """
        count = await repository_of(self).count(self._id.as_filter | conditions)
        return count == 1

    async def upsert(self, **values):
        """
        Upserts a document in the collection.

        Args:
            **values: The key-value pairs to update or insert.

        Returns:
            None

        Raises:
            AssertionError: If the upsert operation fails.

        Examples:
            #>>> await user_ref.upsert(profile__first_name="John")
        """
        #  TODO sustituir __ por .
        result = await repository_of(self)._update_one(
            self._id.as_filter, {"$set": values}, upsert=True
        )

        assert result.matched_count or result.upserted_id

    async def update(self, **values):
        """
        Upserts a document in the collection.

        Args:
            **values: The key-value pairs to update or insert.

        Returns:
            None

        Raises:
            AssertionError: If the upsert operation fails.

        Examples:
            #>>> await user_ref.update(profile__first_name="John")
        """
        #  TODO sustituir __ por .
        result = await repository_of(self)._update_one(
            self._id.as_filter, {"$set": values}, upsert=False
        )

        assert result.matched_count  ##  TODO: test

    ## mutate() as doc creara un contexto de mutacion
    ## doc puede ser valuado leido y escrito de diferentes maneras
    ## despues del contexto de mutacion la operacion será realizada


class RepositoryBase(Generic[P]):
    """
    Base class for repositories that interact with a persistent storage.

    Args:
        tablename (str): The name of the table or collection in the persistent storage.

    Attributes:
        tablename (str): The name of the table or collection in the persistent storage.
        type_adapter (TypeAdapter): The type adapter used for serializing and deserializing objects.
        list_type_adapter (TypeAdapter): The type adapter used for serializing and deserializing lists of objects.
    """

    Cursor: ClassVar[type[Cursor]]

    __persistent__: type[P]
    __qualname__ = "Persistent.Repository"

    def __init__(self, tablename: str):
        self.tablename = tablename

    @cached_property
    def type_adapter(self):
        return TypeAdapter(persistent_cls_of(self))

    @cached_property
    def list_type_adapter(self):
        return TypeAdapter(list[persistent_cls_of(self)])

    def _get_collection_and_session(self):
        session = current_session()
        col = session.persistence.database.get_collection(self.tablename)
        return col, session._inner

    async def _create_index(self, *args, **kwargs):
        """Creates an index on the collection."""
        collection, session = self._get_collection_and_session()
        await collection.create_index(*args, session=session, **kwargs)

    async def _count_documents(self, *args, **kwargs):
        collection, session = self._get_collection_and_session()
        return await collection.count_documents(*args, session=session, **kwargs)

    async def _find_one(self, *args, **kwargs) -> P | None:
        collection, session = self._get_collection_and_session()
        raw = await collection.find_one(*args, session=session, **kwargs)
        return None if raw is None else self.type_adapter.validate_python(raw)

    async def _update_one(self, *args, **kwargs):
        collection, session = self._get_collection_and_session()
        result = await collection.update_one(*args, session=session, **kwargs)
        assert result.acknowledged
        # if not result.acknowledged:
        # raise Exception("Operation not acknowledged")
        return result

    def _find(self, *args, **kwargs) -> Cursor[P]:
        collection, session = self._get_collection_and_session()
        inner_cursor = collection.find(*args, session=session, **kwargs)
        return self.Cursor(inner_cursor, self.type_adapter.validate_python)

    async def insert_obj(self, obj: P):
        "Create ... and assigns a new id to the object"
        collection, session = self._get_collection_and_session()
        raw = self.type_adapter.dump_python(obj, by_alias=True)
        if raw["_id"] is None:
            del raw["_id"]

        result = await collection.insert_one(raw, session=session)

        obj.id = Id(result.inserted_id)

    async def fetch_by_id(self, id: Id, not_found_error=True) -> P | None:
        """
        Read a document from the collection by its ID.

        Args:
            id (Id): The ID of the document to fetch.
            not_found_error (bool, optional): Whether to raise a NotFoundError if the document is not found.
                                                Defaults to True.

        Returns:
            P | None: The fetched document, or None if it is not found.
        """
        collection, session = self._get_collection_and_session()
        raw = await collection.find_one(id.as_filter, session=session)

        if raw is None:
            if not_found_error:
                raise NotFoundError(id)
            return None

        return self.type_adapter.validate_python(raw)

    async def update_obj(self, obj: P):
        """
        Update an object in the database.

        Args:
            obj (P): The object to be updated.

        Raises:
            ValueError: If the object has not been inserted yet.
            NotFoundError: If the object's reference is not found in the database.
        """
        if obj.id is None:
            raise ValueError(f"You are trying to update a non-inserted object")

        raw = self.type_adapter.dump_python(obj, by_alias=True, exclude="id")
        result = await self._update_one(obj.id.as_filter, {"$set": raw})
        if result.matched_count != 1:
            raise NotFoundError(obj.ref)

    async def delete_by_id(self, id: Id, not_found_error=True):
        """
        Delete a document from the collection by its ID.

        Args:
            id (Id): The ID of the document to delete.
            not_found_error (bool, optional): Whether to raise a KeyError if the document is not found.
                                                Defaults to True.

        Raises:
            KeyError: If the document is not found and `not_found_error` is True.
        """
        collection, session = self._get_collection_and_session()

        result = await collection.delete_one(id.as_filter, session=session)
        if not_found_error and result.deleted_count != 1:
            raise KeyError(id)

    async def count(self, filter: Mapping[str, Any] = {}):
        """
        Counts the number of documents in the collection that match the given filter.

        Args:
            filter (Mapping[str, Any], optional): The filter to apply when counting documents. Defaults to {}.

        Returns:
            int: The number of documents that match the filter.
        """
        collection, session = self._get_collection_and_session()
        return await collection.count_documents(filter, session=session)

    def find(self, filter: Mapping[str, Any] = {}) -> Cursor[P]:
        """
        Find documents in the collection based on the provided filter.

        Args:
            filter (Mapping[str, Any], optional): The filter to apply when searching for documents. Defaults to {}.

        Returns:
            Cursor[P]: A cursor object containing the matching documents.
        """
        collection, session = self._get_collection_and_session()
        inner_cursor = collection.find(filter, session=session)
        return self.Cursor(inner_cursor, self.type_adapter.validate_python)


class Cursor(Generic[P]):
    """
    Represents a cursor for iterating over a collection of items.

    Args:
        _inner (AgnosticCursor): The inner cursor implementation.
        item_parser (Callable[[Any], P]): A callable function used to parse each item returned by the cursor.

    Attributes:
        _inner (AgnosticCursor): The inner cursor implementation.
        _item_parser (Callable[[Any], P]): A callable function used to parse each item returned by the cursor.
    """

    __slots__ = "_inner", "_item_parser"

    def __init__(self, _inner: AgnosticCursor, item_parser: Callable[[Any], P]):
        self._inner = _inner
        self._item_parser = item_parser

    def __aiter__(self):
        return self

    async def __anext__(self) -> P:
        raw = await self._inner.next()
        return self._item_parser(raw)

    def skip(self, *args, **kwargs) -> Self:
        cls = type(self)
        return cls(self._inner.skip(*args, **kwargs), self._item_parser)

    def limit(self, *args, **kwargs) -> Self:
        cls = type(self)
        return cls(self._inner.limit(*args, **kwargs), self._item_parser)

    async def as_list(self):
        return [x async for x in self]


Persistent.Id = Id
Persistent.Ref = RefBase
Persistent.Repository = RepositoryBase
RepositoryBase.Cursor = Cursor


def repository_of(
    value: P | type[P] | RefBase[P] | type[RefBase[P]],
) -> RepositoryBase[P]:
    """
    Returns the repository instance of P, given a P or P.Ref type or instance.

    Args:
        value (P | type[P] | RefBase[P] | type[RefBase[P]]): The value of type P or P.Ref.

    Returns:
        RepositoryBase[P]: The repository instance of type P.

    """
    return value.__repository__


def persistent_cls_of(
    value: RefBase[P] | type[RefBase[P]] | RepositoryBase[P] | type[RepositoryBase[P]],
) -> type[P]:
    """
    Returns the persistent class type given a repository or reference type or instance.

    Args:
        value (RefBase[P] | type[RefBase[P]] | RepositoryBase[P] | type[RepositoryBase[P]]):
            A repository or reference type or instance.

    Returns:
        The persistent class type.

    """
    return value.__persistent__
