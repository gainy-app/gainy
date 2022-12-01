def _get_deposit_data(ref_id=None, account_ref_id=None, status=None):
    return {
        "id": ref_id,
        "status": {
            "id": 1,
            "message": status,
            "updated": "2017-11-10T14:43:26.497Z"
        },
        "accountDetails": {
            "accountID": account_ref_id
        },
        "created": "2017-11-10T14:43:26.497Z"
    }


def _get_redemption_data(ref_id=None, account_ref_id=None, status=None):
    return {
        "id": ref_id,
        "status": {
            "id": 1,
            "message": status,
            "updated": "2017-11-10T14:43:26.497Z"
        },
        "accountDetails": {
            "accountID": account_ref_id
        },
        "created": "2017-11-10T14:43:26.497Z"
    }


def mock_create_deposit(amount,
                        account,
                        bank_account,
                        ref_id=None,
                        status=None):

    def mock(_amount, _account, _bank_account):
        assert _amount == amount
        assert _account == account
        assert _bank_account == bank_account
        return _get_deposit_data(ref_id=ref_id, status=status)

    return mock


def mock_create_redemption(amount,
                           account,
                           bank_account,
                           ref_id=None,
                           status=None):

    def mock(_amount, _account, _bank_account):
        assert _amount == amount
        assert _account == account
        assert _bank_account == bank_account
        return {
            "id": ref_id,
            "status": {
                "id": 1,
                "message": status,
                "updated": "2017-11-10T14:43:26.497Z"
            },
            "created": "2017-11-10T14:43:26.497Z"
        }

    return mock


def mock_get_deposit(ref_id=None, account_ref_id=None, status=None):

    def mock(_deposit_ref_id):
        assert _deposit_ref_id == ref_id
        return _get_deposit_data(ref_id, account_ref_id, status=status)

    return mock


def mock_get_redemption(ref_id=None, account_ref_id=None, status=None):

    def mock(_redemption_ref_id):
        assert _redemption_ref_id == ref_id
        return _get_redemption_data(ref_id, account_ref_id, status=status)

    return mock
