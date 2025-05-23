import pytest
from unittest.mock import patch, MagicMock
from io import BytesIO
import os
from minio.error import S3Error

import src.pipeline.core.minio_client as minio_client

@pytest.fixture(autouse=True)
def setup_env(monkeypatch):
    """Setup test environment with mocked MinIO client"""
    # Mock environment variables
    monkeypatch.setenv("MINIO_URL", "localhost:9000")
    monkeypatch.setenv("MINIO_USER", "minioadmin")
    monkeypatch.setenv("MINIO_PASSWORD", "minioadmin")
    
    # Create a mock MinIO client
    mock_client = MagicMock()
    
    # Mock bucket operations
    mock_client.bucket_exists.return_value = False
    
    # Create mock buckets with proper name attributes
    bucket1 = MagicMock()
    bucket1.name = "test-bucket-1"
    bucket2 = MagicMock()
    bucket2.name = "test-bucket-2"
    mock_client.list_buckets.return_value = [bucket1, bucket2]
    
    # Mock object operations
    mock_client.list_objects.return_value = [
        MagicMock(object_name="file1.parquet"),
        MagicMock(object_name="file2.parquet"),
        MagicMock(object_name="file3.txt")
    ]
    
    # Patch the MinIO client
    monkeypatch.setattr(minio_client, 'client', mock_client)
    
    return mock_client

def test_create_bucket_success(setup_env):
    """Test successful bucket creation"""
    mock_client = setup_env
    
    # Test bucket creation
    minio_client.create_bucket("test-bucket")
    
    # Verify bucket creation was attempted
    mock_client.bucket_exists.assert_called_once_with("test-bucket")
    mock_client.make_bucket.assert_called_once_with("test-bucket")

def test_create_bucket_already_exists(setup_env):
    """Test bucket creation when bucket already exists"""
    mock_client = setup_env
    mock_client.bucket_exists.return_value = True
    
    # Test bucket creation
    minio_client.create_bucket("test-bucket")
    
    # Verify bucket existence was checked but not created
    mock_client.bucket_exists.assert_called_once_with("test-bucket")
    mock_client.make_bucket.assert_not_called()

def test_create_bucket_error(setup_env):
    """Test bucket creation with error handling"""
    mock_client = setup_env
    # Create a proper S3Error with all required arguments
    mock_client.bucket_exists.side_effect = S3Error(
        code="TestError",
        message="Test error",
        resource="test-bucket",
        request_id="test-request",
        host_id="test-host",
        response=MagicMock()
    )
    
    # Test bucket creation
    minio_client.create_bucket("test-bucket")
    
    # Verify error was handled gracefully
    mock_client.bucket_exists.assert_called_once_with("test-bucket")
    mock_client.make_bucket.assert_not_called()

def test_delete_all_buckets_success(setup_env):
    """Test successful deletion of all buckets"""
    mock_client = setup_env
    
    # Test bucket deletion
    minio_client.delete_all_buckets()
    
    # Verify all buckets were deleted
    assert mock_client.list_buckets.call_count == 1
    assert mock_client.remove_bucket.call_count == 2
    
    # Get the actual bucket names from the mock calls
    bucket_calls = [call[0][0] for call in mock_client.remove_bucket.call_args_list]
    assert bucket_calls[0] == "test-bucket-1"
    assert bucket_calls[1] == "test-bucket-2"

def test_delete_all_buckets_error(setup_env):
    """Test bucket deletion with error handling"""
    mock_client = setup_env
    # Create a proper S3Error with all required arguments
    mock_client.list_buckets.side_effect = S3Error(
        code="TestError",
        message="Test error",
        resource="test-bucket",
        request_id="test-request",
        host_id="test-host",
        response=MagicMock()
    )
    
    # Test bucket deletion
    minio_client.delete_all_buckets()
    
    # Verify error was handled gracefully
    mock_client.list_buckets.assert_called_once()
    mock_client.remove_bucket.assert_not_called()

def test_upload_data_success(setup_env):
    """Test successful data upload"""
    mock_client = setup_env
    
    # Test data upload
    test_data = b"test data"
    minio_client.upload_data("test-bucket", "test-file.parquet", test_data)
    
    # Verify upload was attempted
    mock_client.bucket_exists.assert_called_once_with("test-bucket")
    mock_client.put_object.assert_called_once()
    
    # Verify correct bucket and file name were used
    call_args = mock_client.put_object.call_args[0]
    assert call_args[0] == "test-bucket"
    assert call_args[1] == "test-file.parquet"
    
    # Verify data was converted to BytesIO
    assert isinstance(call_args[2], BytesIO)
    assert call_args[2].getvalue() == test_data

def test_upload_data_error(setup_env):
    """Test data upload with error handling"""
    mock_client = setup_env
    # Create a proper S3Error with all required arguments
    mock_client.put_object.side_effect = S3Error(
        code="TestError",
        message="Test error",
        resource="test-bucket",
        request_id="test-request",
        host_id="test-host",
        response=MagicMock()
    )
    
    # Test data upload
    test_data = b"test data"
    minio_client.upload_data("test-bucket", "test-file.parquet", test_data)
    
    # Verify error was handled gracefully
    mock_client.bucket_exists.assert_called_once_with("test-bucket")
    mock_client.put_object.assert_called_once()

def test_list_parquet_files(setup_env):
    """Test listing parquet files"""
    mock_client = setup_env
    
    # Test listing parquet files
    files = minio_client.list_parquet_files("test-bucket")
    
    # Verify correct files were returned
    assert len(files) == 2
    assert "file1.parquet" in files
    assert "file2.parquet" in files
    assert "file3.txt" not in files
    
    # Verify list_objects was called with correct parameters
    mock_client.list_objects.assert_called_once_with("test-bucket", prefix="", recursive=True)

def test_list_parquet_files_with_prefix(setup_env):
    """Test listing parquet files with prefix"""
    mock_client = setup_env
    
    # Test listing parquet files with prefix
    files = minio_client.list_parquet_files("test-bucket", prefix="test/")
    
    # Verify list_objects was called with correct parameters
    mock_client.list_objects.assert_called_once_with("test-bucket", prefix="test/", recursive=True) 