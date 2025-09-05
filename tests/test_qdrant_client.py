import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock
from qdrant_client.http.models import PointStruct
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import service.qdrant_client as qdrant_client_module

@patch("service.qdrant_client.QdrantClient") 
def test_QdrantClientService_init_and_connect_success(mock_qdrant_client):
    mock_instance = MagicMock()
    mock_qdrant_client.return_value = mock_instance
    mock_instance.collection_exists.return_value = True

    service = qdrant_client_module.QdrantClientService(host="http://localhost:6333")

    result = service.connect()

    assert result is True
    assert service._client == mock_instance
    mock_qdrant_client.assert_called_once_with(url="http://localhost:6333", https=False, timeout=120)

@patch("service.qdrant_client.QdrantClient")
def test_QdrantClientService_connect_fail(mock_qdrant_client):
    mock_qdrant_client.side_effect = Exception("Connection failed")

    service = qdrant_client_module.QdrantClientService(host="http://localhost:6333")

    result = service.connect()

    assert result is False
    assert service._client is None

@patch("service.qdrant_client.QdrantClient")
def test_ensure_collection_exists_already_exists(mock_qdrant_client):
    mock_instance = MagicMock()
    mock_qdrant_client.return_value = mock_instance
    mock_instance.collection_exists.return_value = True

    service = qdrant_client_module.QdrantClientService(host="http://localhost:6333", )
    service._client = mock_instance

    result = service._ensure_collection()

    assert result is None
    mock_instance.collection_exists.assert_called_once_with(collection_name="desa-maju-rag")
    mock_instance.create_collection.assert_not_called()

@patch("service.qdrant_client.QdrantClient")
def test_ensure_collection_failed(mock_qdrant_client):
    mock_instance = MagicMock()
    mock_qdrant_client.return_value = mock_instance
    mock_instance.collection_exists.side_effect = Exception("DB error")

    service = qdrant_client_module.QdrantClientService(host="http://localhost:6333")
    service._client = mock_instance

    with pytest.raises(Exception, match="DB error"):
        service._ensure_collection()

    mock_instance.create_collection.assert_not_called()

@patch("service.qdrant_client.QdrantClient")
def test_create_collection_success(mock_qdrant_client):
    mock_instance = MagicMock()
    mock_qdrant_client.return_value = mock_instance

    service = qdrant_client_module.QdrantClientService("http://localhost:6333")
    service._client = mock_instance

    service._create_collection()

    mock_instance.create_collection.assert_called_once()

@patch("service.qdrant_client.QdrantClient")
def test_create_collection_fail(mock_qdrant_client):
    mock_instance = MagicMock()
    mock_qdrant_client.return_value = mock_instance
    mock_instance.create_collection.side_effect = Exception("Creation failed")

    service = qdrant_client_module.QdrantClientService("http://localhost:6333")
    service._client = mock_instance

    with pytest.raises(Exception, match="Creation failed"):
        service._create_collection()

    mock_instance.create_collection.assert_called_once()

@patch("service.qdrant_client.QdrantClient")
def test_create_points_success(mock_qdrant_client):
    mock_instance = MagicMock()
    mock_qdrant_client.return_value = mock_instance

    bm25_vector = MagicMock()
    bm25_vector.indices.tolist.return_value = [0, 1]
    bm25_vector.values.tolist.return_value = [0.5, 0.7]

    colbert_vector = [MagicMock()]
    colbert_vector[0].tolist.return_value = [0.1, 0.2]

    gemini_vector = [0.9, 0.8]

    chunk = MagicMock()
    chunk.meta.doc_items = [MagicMock()]
    chunk.meta.doc_items[0].prov = [MagicMock()]
    chunk.meta.doc_items[0].prov[0].page_no = 5

    processed_data = {
        "bm25_vectors": [bm25_vector],
        "colbert_vectors": [colbert_vector],
        "gemini_vectors": [gemini_vector],
        "docs": ["Doc text"],
        "chunks": [chunk],
        "filename": "test.pdf"
    }

    service = qdrant_client_module.QdrantClientService("http://localhost:6333")
    service._client = mock_instance
    points = service.create_points(processed_data, start_id=100)

    # Assert
    assert len(points) == 1
    point = points[0]
    assert isinstance(point, PointStruct)
    assert point.id == 100
    assert point.payload["document"] == "Doc text"
    assert point.payload["filename"] == "test.pdf"
    assert point.payload["page_number"] == 5
    assert "upload_timestamp" in point.payload

@patch("service.qdrant_client.QdrantClient")
def test_create_points_fail_missing_key(mock_qdrant_client):
    mock_instance = MagicMock()
    mock_qdrant_client.return_value = mock_instance
    service = qdrant_client_module.QdrantClientService("http://localhost:6333")
    service._client = mock_instance
    processed_data = {
        "docs": [],
        "chunks": [],
        "filename": "test.pdf"
    }

    with pytest.raises(KeyError):
        service.create_points(processed_data, start_id=1)

@patch("service.qdrant_client.QdrantClient")
def test_get_next_id_success(mock_qdrant_client):
    # Arrange
    mock_instance = MagicMock()
    mock_qdrant_client.return_value = mock_instance
    mock_instance.get_collection.return_value.points_count = 42

    service = qdrant_client_module.QdrantClientService("http://localhost:6333")
    service._client = mock_instance

    # Act
    result = service.get_next_id()

    # Assert
    assert result == 42
    mock_instance.get_collection.assert_called_once_with(collection_name="desa-maju-rag")

@patch("service.qdrant_client.QdrantClient")
def test_get_next_id_fail(mock_qdrant_client):
    # Arrange
    mock_instance = MagicMock()
    mock_qdrant_client.return_value = mock_instance
    mock_instance.get_collection.side_effect = Exception("DB error")

    service = qdrant_client_module.QdrantClientService("http://localhost:6333")
    service._client = mock_instance

    # Act
    result = service.get_next_id()

    # Assert → kalau error harus return 0
    assert result == 0
    mock_instance.get_collection.assert_called_once_with(collection_name="desa-maju-rag")


