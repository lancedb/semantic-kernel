from logging import Logger

from typing import Optional

from semantic_kernel.memory.memory_record import MemoryRecord
from semantic_kernel.memory.memory_store_base import MemoryStoreBase
from semantic_kernel.utils.null_logger import NullLogger

from lancedb import LanceDBConnection, LanceTable

# MemoryRecord(
#     is_reference=metadata["is_reference"],
#     external_source_name=metadata["external_source_name"],
#     id=metadata["id"],
#     description=metadata["description"],
#     text=document,
#     embedding=embedding,
#     additional_metadata=metadata["additional_metadata"],
#     key=id,
#     timestamp=metadata["timestamp"],
# )

class LanceDBMemoryStore(MemoryStoreBase):
    _logger: Logger
    _db: LanceDBConnection
    _table: LanceTable

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
        self._table = self._db.table(self.table_name)


    async def upsert_async(self, collection_name: str, record: MemoryRecord) -> str:
        pass

    async def upsert_batch_async(
        self, collection_name: str, records: List[MemoryRecord]
    ) -> List[str]:
        pass

    async def get_async(
        self, collection_name: str, key: str
    ) -> MemoryRecord:
        raw_records = self._table.search([]).where(f'id == ${key}')

    async def get_batch_async(
        self, collection_name: str, keys: List[str]
    ) -> List[MemoryRecord]:
        raw_records = self._table.search([]).where(f'id == ${key}')

    # TODO: Implement
    async def remove_async(self, collection_name: str, key: str) -> None:
        pass
    
    # TODO: Implement
    async def remove_batch_async(self, collection_name: str, keys: List[str]) -> None:
        pass

    async def get_nearest_matches_async(
        self,
        collection_name: str,
        embedding: ndarray,
        limit: int,
    ) -> List[Tuple[MemoryRecord, float]]:
        raw_records = self._table.search(embedding).limit(limit)

    async def get_nearest_match_async(
        self,
        collection_name: str,
        embedding: ndarray, 
        min_relevance_score: float,
    ) -> Tuple[MemoryRecord, float]:
        raw_records = self._table.search(embedding).limit(1)

