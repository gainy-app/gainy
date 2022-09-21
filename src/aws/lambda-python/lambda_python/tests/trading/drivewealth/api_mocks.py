def mock_create_deposit(amount,
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
