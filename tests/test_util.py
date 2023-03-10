import pytest

from app.util import convert_json


@pytest.mark.parametrize("val", [
    r'{name:Super   Table}',
    r'{ name:Super   Table }',
    r'{  name:  Super   Table  }',
    r'{name: Super   Table}',
    r'{name : Super   Table}',
    r'{name  :  Super   Table}',
    r'{  name  :  Super   Table  }',
])
def test_convert_json(val: str):
    expected = r'{"name":"Super   Table"}'
    converted = convert_json(val)
    print(f'{val=}')
    print(f'{converted=}')
    print(f'{expected=}')
    assert converted == expected


@pytest.mark.parametrize("val", [
    r'{  name: Super   Table    ,keys:{created_at:int,description: fff}}',
    r'{ name:Super   Table,keys : {created_at:int,description: fff  }}',
    r'{ name:   Super   Table   ,   keys : {   created_at  : int , description : fff }  }',
    r'{ name:Super   Table   ,   keys : {   created_at: int , description :fff  }  }',
])
def test_convert_long_json(val: str):
    expected = r'{"name":"Super   Table","keys":{"created_at":"int","description":"fff"}}'
    converted = convert_json(val)
    print(f'{val=}')
    print(f'{converted=}')
    print(f'_{expected=}')
    assert converted == expected
