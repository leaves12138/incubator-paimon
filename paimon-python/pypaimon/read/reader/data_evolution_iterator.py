"""
DataEvolutionIterator implementation for iterating over combined rows from multiple readers.

This iterator assumes that all iterators are aligned, and as long as one returns null,
the others will also have no data.
"""

from typing import List, Optional
from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.data_evolution_row import DataEvolutionRow
from pypaimon.table.row.internal_row import InternalRow


class DataEvolutionIterator(RecordIterator):
    """
    The batch which is made up by several batches, it assumes that all iterators are aligned,
    and as long as one returns null, the others will also have no data.
    """

    def __init__(self, row: DataEvolutionRow, iterators: List[Optional[RecordIterator]]):
        self.row = row
        self.iterators = iterators

    def next(self) -> Optional[InternalRow]:
        for i, iterator in enumerate(self.iterators):
            if iterator is not None:
                next_row = iterator.next()
                if next_row is None:
                    return None
                self.row.set_row(i, next_row)
        return self.row

    def release_batch(self) -> None:
        for iterator in self.iterators:
            if iterator is not None:
                iterator.release_batch()
