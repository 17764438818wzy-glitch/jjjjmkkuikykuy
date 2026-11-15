import pytest

from paperflow_v3.control.auto_capacity import checked_seats, SeatCapacityError


def route():
    return {'seat_egress': {
        'protocol': 'fixed-seat-egress-v1', 'configured_capacity': 50,
        'enabled_capacity': 50, 'per_domestic_uid_limit': 2,
        'groups': [
            {'id': 'local', 'first': 1, 'last': 45, 'capacity': 45, 'enabled': True, 'verified': True},
            {'id': 'aliyun', 'first': 46, 'last': 50, 'capacity': 5, 'enabled': True, 'verified': True},
        ],
    }}


@pytest.mark.parametrize('local_count,aliyun_count', [(30, 20), (45, 5)])
def test_explicit_user_allocation_can_pass_start_capacity_validation(local_count, aliyun_count):
    snapshot = route()
    snapshot['seat_egress']['groups'][0].update(last=local_count, capacity=local_count)
    snapshot['seat_egress']['groups'][1].update(first=local_count + 1, capacity=aliyun_count)
    value = checked_seats(snapshot)
    assert value['enabled_capacity'] == 50
    assert value['per_domestic_uid_limit'] == 2
    assert [(row['id'], row['capacity']) for row in value['groups']] == [('local', local_count), ('aliyun', aliyun_count)]


@pytest.mark.parametrize('first,last,capacity', [(45, 50, 6), (47, 50, 4), (46, 51, 6), (True, 50, 5), (46, 50, 10)])
def test_invalid_layout_cannot_pass_start_capacity_validation(first, last, capacity):
    value = route()
    value['seat_egress']['groups'][1].update(first=first, last=last, capacity=capacity)
    with pytest.raises(SeatCapacityError):
        checked_seats(value)


def test_unknown_egress_group_cannot_masquerade_as_verified_aliyun():
    value = route()
    value['seat_egress']['groups'][1]['id'] = 'unexpected'
    with pytest.raises(SeatCapacityError):
        checked_seats(value)


def test_duplicate_group_identity_is_rejected():
    value = route()
    value['seat_egress']['groups'][1]['id'] = 'local'
    with pytest.raises(SeatCapacityError):
        checked_seats(value)


def test_enabled_unverified_egress_is_rejected():
    value = route()
    value['seat_egress']['groups'][1]['verified'] = False
    with pytest.raises(SeatCapacityError):
        checked_seats(value)


def test_original_25_15_10_layout_remains_supported():
    value = route()
    value['seat_egress']['groups'] = [
        {'id': group, 'first': first, 'last': last, 'capacity': capacity, 'enabled': True, 'verified': True}
        for group, first, last, capacity in [('local', 1, 25, 25), ('bwg', 26, 40, 15), ('aliyun', 41, 50, 10)]
    ]
    assert checked_seats(value)['enabled_capacity'] == 50
