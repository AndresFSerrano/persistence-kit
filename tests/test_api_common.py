from persistence_kit.api.common import ApiError, pagination_params


def test_api_error_model():
    err = ApiError(detail="Not found")
    assert err.detail == "Not found"


def test_pagination_params_passthrough():
    assert pagination_params(5, 10) == (5, 10)
