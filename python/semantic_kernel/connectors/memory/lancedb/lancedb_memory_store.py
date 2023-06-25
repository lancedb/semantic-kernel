from logging import Logger

from typing import Optional, List, Tuple

from semantic_kernel.memory.memory_record import MemoryRecord
from semantic_kernel.memory.memory_store_base import MemoryStoreBase
from semantic_kernel.utils.null_logger import NullLogger

from lancedb import LanceDBConnection
from numpy import ndarray

class LanceDBMemoryStore(MemoryStoreBase):
    _logger: Logger
    _db: LanceDBConnection

    def __init__(
            self,
            logger: Optional[Logger] = None,
            uri: str = "/tmp/lancedb",
            table_name: str = "table_name"
    ) -> None:
        try:
            import lancedb

        except ImportError:
            raise ValueError(
                "Could not import lancedb python package. "
                "Please install it with `pip install lancedb`."
            )
        
        self._logger = logger or NullLogger()
        self._db = lancedb.connect(uri)
    
    async def create_collection_async(self, collection_name: str) -> None:
        self._db.create_table(collection_name)
    
    async def get_collection_async(
        self, collection_name: str
    ) -> Optional[any]:
        if collection_name in self._db.table_names():
            return self._db.open_table(collection_name)
        else:
            return None
    
    async def get_collections_async(self) -> List[str]:
        return self._db.table_names()
    
    async def delete_collection_async(self, collection_name: str) -> None:
        self._db.drop_table(collection_name)
    
    async def does_collection_exist_async(self, collection_name: str) -> bool:
        if collection_name in self._db.table_names():
            return True
        else:
            return False

    async def upsert_async(self, collection_name: str, record: MemoryRecord) -> str:
        table = await self.get_collection_async(collection_name)

        if table is None:
            raise Exception(f"Table '{collection_name}' does not exist")

        data = {
            "id": record._id or "",
            "vector": record._embedding or [],
            "text": record._text or "",
            "timestamp": record._timestamp or "",
            "is_reference": record._is_reference,
            "external_source_name": record._external_source_name or "",
            "description": record._description or "",
            "additional_metadata": record._additional_metadata or "",
        }
        table.add([data])

        return data._id

    async def upsert_batch_async(
        self, collection_name: str, records: List[MemoryRecord]
    ) -> List[str]:
        table = await self.get_collection_async(collection_name)

        if table is None:
            raise Exception(f"Table '{collection_name}' does not exist")
        
        ids = []
        datas = []
        
        for record in records:
            data = {
                "id": record._id or "",
                "vector": record._embedding or [],
                "text": record._text or "",
                "timestamp": record._timestamp or "",
                "is_reference": record._is_reference,
                "external_source_name": record._external_source_name or "",
                "description": record._description or "",
                "additional_metadata": record._additional_metadata or "",
            }
            datas.append(data)
            ids.append(data._id)
        
        table.add(datas)

        return ids

    async def get_async(
        self, collection_name: str, key: str
    ) -> MemoryRecord:
        table = await self.get_collection_async(collection_name)

        if table is None:
            raise Exception(f"Table '{collection_name}' does not exist")
        
        raw_record = table.search([]).where(f'id == ${key}')
        record = MemoryRecord(
            is_reference=raw_record["is_reference"],
            external_source_name=raw_record["external_source_name"],
            id=raw_record["id"],
            description=raw_record["description"],
            text=raw_record["text"],
            embedding=raw_record["vector"],
            additional_metadata=raw_record["additional_metadata"],
            key=id,
            timestamp=raw_record["timestamp"],
        )
        return record

    async def get_batch_async(
        self, collection_name: str, keys: List[str]
    ) -> List[MemoryRecord]:
        table = await self.get_collection_async(collection_name)

        if table is None:
            raise Exception(f"Table '{collection_name}' does not exist")

        raw_records = self._table.search([]).where(f'id IN ${[keys]}')
        records = []
        for raw_record in raw_records:
            record = MemoryRecord(
                is_reference=raw_record["is_reference"],
                external_source_name=raw_record["external_source_name"],
                id=raw_record["id"],
                description=raw_record["description"],
                text=raw_record["text"],
                embedding=raw_record["vector"],
                additional_metadata=raw_record["additional_metadata"],
                key=id,
                timestamp=raw_record["timestamp"],
            )
            records.append(record)
        
        return records

    async def remove_async(self, collection_name: str, key: str) -> None:
        table = await self.get_collection_async(collection_name)

        if table is None:
            raise Exception(f"Table '{collection_name}' does not exist")
        
        table.delete(f'id == ${key}')
    
    async def remove_batch_async(self, collection_name: str, keys: List[str]) -> None:
        table = await self.get_collection_async(collection_name)

        if table is None:
            raise Exception(f"Table '{collection_name}' does not exist")
        
        table.delete(f'id in ${[keys]}')

    async def get_nearest_matches_async(
        self,
        collection_name: str,
        embedding: ndarray,
        limit: int,
    ) -> List[Tuple[MemoryRecord, float]]:
        table = await self.get_collection_async(collection_name)

        if table is None:
            raise Exception(f"Table '{collection_name}' does not exist")

        raw_records = self._table.search(embedding).limit(limit)

        records = []
        for raw_record in raw_records:
            record = MemoryRecord(
                is_reference=raw_record["is_reference"],
                external_source_name=raw_record["external_source_name"],
                id=raw_record["id"],
                description=raw_record["description"],
                text=raw_record["text"],
                embedding=raw_record["vector"],
                additional_metadata=raw_record["additional_metadata"],
                key=id,
                timestamp=raw_record["timestamp"],
            )
            records.append((record, raw_record["score"]))
        
        return records

    async def get_nearest_match_async(
        self,
        collection_name: str,
        embedding: ndarray, 
        min_relevance_score: float,
    ) -> Tuple[MemoryRecord, float]:
        table = await self.get_collection_async(collection_name)

        if table is None:
            raise Exception(f"Table '{collection_name}' does not exist")

        raw_record = self._table.search(embedding).limit(1)

        record = MemoryRecord(
            is_reference=raw_record["is_reference"],
            external_source_name=raw_record["external_source_name"],
            id=raw_record["id"],
            description=raw_record["description"],
            text=raw_record["text"],
            embedding=raw_record["vector"],
            additional_metadata=raw_record["additional_metadata"],
            key=id,
            timestamp=raw_record["timestamp"],
        )
        return (record, raw_record["score"])
    
if __name__ == "__main__":
    import asyncio

    import numpy as np

    memory = LanceDBMemoryStore()
    memory_record1 = MemoryRecord(
        id="test_id1",
        text="sample text1",
        is_reference=False,
        embedding=np.array([0.5, 0.5]),
        description="description",
        external_source_name="external source",
        timestamp="timestamp",
        additional_metadata=["blah"]
    )
    memory_record2 = MemoryRecord(
        id="test_id2",
        text="sample text2",
        is_reference=False,
        embedding=np.array([0.25, 0.75]),
        description="description",
        external_source_name="external source",
        timestamp="timestamp",
        additional_metadata=["blah"]
    )

    asyncio.run(memory.create_collection_async("test_table"))

    asyncio.run(
        memory.upsert_batch_async("test_table", [memory_record1, memory_record2])
    )

    result = asyncio.run(memory.get_async("test_table", "test_id1"))
    results = asyncio.run(
        memory.get_nearest_match_async("test_table", np.array([0.5, 0.5]))
    )
    print(results)
