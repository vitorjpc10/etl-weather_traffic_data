from scripts.data_extraction import get_traffic, get_weather


def test_get_weather():
    data = get_weather('New York')
    assert 'main' in data
    assert 'temperature' in data['main']

def test_get_traffic():
    data = get_traffic('New York, NY', 'Los Angeles, CA')
    assert 'routes' in data
    assert len(data['routes']) > 0
