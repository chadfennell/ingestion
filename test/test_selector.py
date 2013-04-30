from dplaingestion.selector import getprop, copy, PATH_DELIM, setprop, exists


TEST_OBJ = {
    'person': {
        'name': 'john smith',
        'age': 25
    },
    'address': {
        'city': 'ottawa',
        'street': '22 acacia'
    },
    'geek-code': 'GE d? s: a+'
}
def test_get():
    assert getprop(TEST_OBJ,PATH_DELIM.join(('person','name'))) == TEST_OBJ['person']['name']
    assert getprop(TEST_OBJ,PATH_DELIM.join(('person','age'))) == TEST_OBJ['person']['age']
    assert getprop(TEST_OBJ,'address') == TEST_OBJ['address']
    assert getprop(TEST_OBJ,'geek-code') == TEST_OBJ['geek-code']
    assert getprop(TEST_OBJ,PATH_DELIM.join(('','person','name'))) == TEST_OBJ['person']['name']

    assert exists(TEST_OBJ,PATH_DELIM.join(('person','name')))
    assert exists(TEST_OBJ,PATH_DELIM.join(('person','age')))
    assert exists(TEST_OBJ,'geek-code')
    assert not exists(TEST_OBJ,'kajsdlj')
    assert not exists(TEST_OBJ,PATH_DELIM.join(('person','sex')))

def test_set_string():
    o = copy.deepcopy(TEST_OBJ)
    setprop(o,PATH_DELIM.join(('address','street')),'22 sussex')
    assert getprop(o,PATH_DELIM.join(('address','street'))) == '22 sussex'

def test_set_dict():
    o = copy.deepcopy(TEST_OBJ)
    setprop(o,'person',{'name': 'jason jones','age': 38})
    assert getprop(o,PATH_DELIM.join(('person','age'))) == 38
