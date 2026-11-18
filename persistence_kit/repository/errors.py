class TransactionError(Exception):
    pass


class TransactionRequiredError(TransactionError):
    pass


class TransactionNotSupportedError(TransactionError):
    pass


class RepeatedActionError(TransactionError):
    def __init__(self, key: str) -> None:
        super().__init__(f"The action '{key}' was already done.")
        self.key = key
