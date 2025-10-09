"""
DataEvolutionRow implementation for combining multiple rows with field mapping.

This class represents a row that is composed of multiple inner rows, where each field
can come from a different inner row based on rowOffsets and fieldOffsets arrays.
"""

from typing import List, Optional, Any
from datetime import datetime
from decimal import Decimal
from pypaimon.table.row.internal_row import InternalRow
from pypaimon.table.row.row_kind import RowKind


class DataEvolutionRow(InternalRow):
    """
    A row which is made up by several rows.
    
    This reader, assembling multiple reader into one big and great reader. The row it produces
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

    def __init__(self, row_number: int, row_offsets: List[int], field_offsets: List[int]):
        self.rows: List[Optional[InternalRow]] = [None] * row_number
        self.row_offsets = row_offsets
        self.field_offsets = field_offsets

    def row_number(self) -> int:
        return len(self.rows)

    def set_row(self, pos: int, row: InternalRow) -> None:
        if pos >= len(self.rows):
            raise IndexError(
                f"Position {pos} is out of bounds for rows size {len(self.rows)}"
            )
        self.rows[pos] = row

    def _choose_row(self, pos: int) -> InternalRow:
        return self.rows[self.row_offsets[pos]]

    def _offset_in_row(self, pos: int) -> int:
        return self.field_offsets[pos]

    def get_field(self, pos: int) -> Any:
        """Returns the value at the given position."""
        if self.row_offsets[pos] == -1:
            return None
        return self._choose_row(pos).get_field(self._offset_in_row(pos))

    def __len__(self) -> int:
        """Returns the number of fields in this row."""
        return len(self.field_offsets)

    def get_row_kind(self) -> RowKind:
        return self.rows[0].get_row_kind()

    def is_null_at(self, pos: int) -> bool:
        if self.row_offsets[pos] == -1:
            return True
        return self._choose_row(pos).is_null_at(self._offset_in_row(pos))

    # Typed getter methods to match Java interface
    def get_boolean(self, pos: int) -> bool:
        return self._choose_row(pos).get_field(self._offset_in_row(pos))

    def get_byte(self, pos: int) -> int:
        return self._choose_row(pos).get_field(self._offset_in_row(pos))

    def get_short(self, pos: int) -> int:
        return self._choose_row(pos).get_field(self._offset_in_row(pos))

    def get_int(self, pos: int) -> int:
        return self._choose_row(pos).get_field(self._offset_in_row(pos))

    def get_long(self, pos: int) -> int:
        return self._choose_row(pos).get_field(self._offset_in_row(pos))

    def get_float(self, pos: int) -> float:
        return self._choose_row(pos).get_field(self._offset_in_row(pos))

    def get_double(self, pos: int) -> float:
        return self._choose_row(pos).get_field(self._offset_in_row(pos))

    def get_string(self, pos: int) -> str:
        return self._choose_row(pos).get_field(self._offset_in_row(pos))

    def get_decimal(self, pos: int, precision: int, scale: int) -> Decimal:
        return self._choose_row(pos).get_field(self._offset_in_row(pos))

    def get_timestamp(self, pos: int, precision: int) -> datetime:
        return self._choose_row(pos).get_field(self._offset_in_row(pos))

    def get_binary(self, pos: int) -> bytes:
        return self._choose_row(pos).get_field(self._offset_in_row(pos))

    def get_variant(self, pos: int) -> Any:
        return self._choose_row(pos).get_field(self._offset_in_row(pos))

    def get_blob(self, pos: int) -> Any:
        return self._choose_row(pos).get_field(self._offset_in_row(pos))

    def get_array(self, pos: int) -> Any:
        return self._choose_row(pos).get_field(self._offset_in_row(pos))

    def get_map(self, pos: int) -> Any:
        return self._choose_row(pos).get_field(self._offset_in_row(pos))

    def get_row(self, pos: int, num_fields: int) -> InternalRow:
        return self._choose_row(pos).get_field(self._offset_in_row(pos))
