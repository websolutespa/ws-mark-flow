"""
Small model behaviours not exercised elsewhere: the ``PyObjectId`` validator
and ``ConversionStats.success_rate``.

Note: ``JobStorage.update_job`` / ``IngestionJobStorage.update`` contain an
``if not update_dict`` early return that can never fire (``updated_at`` is
always added first); the resulting behaviour - an empty update only touches
``updated_at`` - is already pinned down in ``test_storage.py``.
"""
import pytest
from bson import ObjectId

from src.models import ConversionStats, PyObjectId


class TestPyObjectId:
    def test_registers_its_validator(self):
        assert list(PyObjectId.__get_validators__()) == [PyObjectId.validate]

    def test_object_id_is_converted_to_its_hex_string(self):
        oid = ObjectId("65a1b2c3d4e5f60718293a4b")

        result = PyObjectId.validate(oid, None)

        assert result == "65a1b2c3d4e5f60718293a4b"
        assert isinstance(result, str)

    def test_valid_hex_string_is_accepted_unchanged(self):
        assert PyObjectId.validate("65a1b2c3d4e5f60718293a4b", None) == "65a1b2c3d4e5f60718293a4b"

    @pytest.mark.parametrize(
        "value",
        ["", "not-an-object-id", "65a1b2c3d4e5f60718293a4", "65a1b2c3d4e5f60718293a4bz", 42, None, b"x" * 13],
    )
    def test_anything_else_is_rejected(self, value):
        with pytest.raises(ValueError, match="Invalid ObjectId"):
            PyObjectId.validate(value, None)


class TestSuccessRate:
    def test_nothing_processed_yet_is_zero(self):
        assert ConversionStats(total_files=5, pending_files=5).success_rate == 0.0

    def test_pending_files_are_excluded(self):
        stats = ConversionStats(total_files=10, pending_files=6, completed_files=3, failed_files=1)

        assert stats.success_rate == 75.0

    def test_skipped_files_count_as_processed_and_rate_is_rounded(self):
        stats = ConversionStats(completed_files=1, failed_files=1, skipped_files=1)

        assert stats.success_rate == 33.33

    def test_all_completed_is_one_hundred(self):
        assert ConversionStats(completed_files=4).success_rate == 100.0
