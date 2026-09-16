from ingestion.producer import transform_earthquake


def test_transform_earthquake():
    earthquake = {
        "id": "test123",
        "properties": {
            "mag": 4.5,
            "place": "Test Location",
            "time": 1234567890000,
        },
        "geometry": {
            "coordinates": [-46.63, -23.55, 10.0]
        },
    }

    result = transform_earthquake(earthquake)

    assert result["event_id"] == "test123"
    assert result["magnitude"] == 4.5
    assert result["place"] == "Test Location"
    assert result["longitude"] == -46.63
    assert result["latitude"] == -23.55
    assert result["depth_km"] == 10.0
    assert result["event_time"] == 1234567890000