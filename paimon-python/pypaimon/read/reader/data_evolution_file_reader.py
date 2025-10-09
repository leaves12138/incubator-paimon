################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""
DataEvolutionFileReader implementation for combining multiple inner readers.

This is a union reader which contains multiple inner readers. The row it produces
also come from the readers it contains.

For example, the expected schema for this reader is : int, int, string, int, string, int.(Total 6 fields)
It contains three inner readers, we call them reader0, reader1 and reader2.

The rowOffsets and fieldOffsets are all 6 elements long the same as
output schema. RowOffsets is used to indicate which inner reader the field comes from, and
fieldOffsets is used to indicate the offset of the field in the inner reader.

For example, if rowOffsets is {0, 2, 0, 1, 2, 1} and fieldOffsets is {0, 0, 1, 1, 1, 0}, it means:
- The first field comes from reader0, and it is at offset 0 in reader0.
- The second field comes from reader2, and it is at offset 0 in reader2.
- The third field comes from reader0, and it is at offset 1 in reader0.
- The fourth field comes from reader1, and it is at offset 1 in reader1.
- The fifth field comes from reader2, and it is at offset 1 in reader2.
- The sixth field comes from reader1, and it is at offset 0 in reader1.

These three readers work together, package out final and complete rows.
"""

from typing import List, Optional
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.data_evolution_row import DataEvolutionRow
from pypaimon.read.reader.data_evolution_iterator import DataEvolutionIterator


class DataEvolutionFileReader(RecordReader):
    """
    This is a union reader which contains multiple inner readers.
    
    This reader, assembling multiple reader into one big and great reader. The row it produces
    also come from the readers it contains.
    """

    def __init__(self, row_offsets: List[int], field_offsets: List[int], readers: List[Optional[RecordReader]]):
        if row_offsets is None:
            raise ValueError("Row offsets must not be null")
        if field_offsets is None:
            raise ValueError("Field offsets must not be null")
        if len(row_offsets) != len(field_offsets):
            raise ValueError("Row offsets and field offsets must have the same length")
        if not row_offsets:
            raise ValueError("Row offsets must not be empty")
        if not readers or len(readers) < 1:
            raise ValueError("Readers should be more than 0")
        self.row_offsets = row_offsets
        self.field_offsets = field_offsets
        self.readers = readers

    def read_batch(self) -> Optional[RecordIterator]:
        data_evolution_row = DataEvolutionRow(len(self.readers), self.row_offsets, self.field_offsets)
        iterators: List[Optional[RecordIterator]] = [None] * len(self.readers)
        for i, reader in enumerate(self.readers):
            if reader is not None:
                batch = reader.read_batch()
                if batch is None:
                    # all readers are aligned, as long as one returns null, the others will also
                    # have no data
                    return None
                iterators[i] = batch
        return DataEvolutionIterator(data_evolution_row, iterators)

    def close(self) -> None:
        try:
            for reader in self.readers:
                if reader is not None:
                    reader.close()
        except Exception as e:
            raise IOError("Failed to close inner readers") from e