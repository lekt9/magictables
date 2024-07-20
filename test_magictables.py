# test_magictables.py

import pytest
import polars as pl
from magictables import mtable, mai
from magictables.query import query
from magictables.database import magic_db
import os


# Mock API function for testing
@mtable()
def mock_api_call(param: str):
    return [{"id": 1, "param": param, "value": f"test_{param}"}]


# Mock AI function for testing
@mai(batch_size=2, mode="augment", query="add a description")
def mock_ai_call(data: pl.DataFrame):
    return data


# Test mtable decorator
def test_mtable_decorator():
    # First call
    result1 = mock_api_call("test1")
    assert isinstance(result1, pl.DataFrame)
    assert len(result1) == 1
    assert result1["param"][0] == "test1"

    # Second call (should be cached)
    result2 = mock_api_call("test1")
    assert result1.equals(result2)

    # Different parameter (should not be cached)
    result3 = mock_api_call("test2")
    assert not result1.equals(result3)


# Test mai decorator
def test_mai_decorator():
    input_data = pl.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    result = mock_ai_call(input_data)
    assert isinstance(result, pl.DataFrame)
    assert "description" in result.columns


# Test query functionality
def test_query():
    # Insert some test data
    magic_db.cache_results(
        "test_table",
        pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]}),
        "test_call_id",
    )

    # Test basic query
    result = query("test_table").all()
    assert len(result) == 3

    # Test filter
    filtered = query("test_table").filter(value="b").all()
    assert len(filtered) == 1
    assert filtered[0]["value"] == "b"

    # Test order_by
    ordered = query("test_table").order_by("-id").all()
    assert ordered[0]["id"] == 3

    # Test limit
    limited = query("test_table").limit(2).all()
    assert len(limited) == 2

    # Test count
    count = query("test_table").count()
    assert count == 3

    # Test first
    first = query("test_table").first()
    assert first["id"] == 1


# Test database operations
def test_database_operations():
    test_df = pl.DataFrame({"id": [1, 2], "value": ["x", "y"]})
    magic_db.cache_results("test_db_ops", test_df, "test_db_call_id")

    # Test get_cached_result
    cached = magic_db.get_cached_result("test_db_ops", "test_db_call_id")
    assert cached.equals(test_df)

    # Test create_table_if_not_exists
    magic_db.create_table_if_not_exists("test_new_table", ["id", "value"])
    assert magic_db.engine.has_table("test_new_table")


# Clean up test database after all tests
@pytest.fixture(scope="session", autouse=True)
def cleanup(request):
    def remove_test_db():
        if os.path.exists("magic.db"):
            os.remove("magic.db")

    request.addfinalizer(remove_test_db)
