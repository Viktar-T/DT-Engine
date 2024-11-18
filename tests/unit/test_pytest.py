# tests/unit/test_pytest.py
def inc(x):
    return x + 1

def test_answer():
    assert inc(3) == 4
