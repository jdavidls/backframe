from pymongo import ASCENDING
from backframe.persistence.core import Persistent, repository_of


class Counter(Persistent):
    codename: str
    value: int

    class Repository(Persistent.Repository):
        async def init_db(self):
            await self._create_index([("codename", ASCENDING)], unique=True)

    @classmethod
    async def inc(cls, codename: str) -> int:
        raw = await repository_of(cls)._find_one_and_update(
            {"codename": codename},
            {"$inc": {"value": 1}},
            upsert=True,
            return_new_document=True,
        )

        return raw["value"]
