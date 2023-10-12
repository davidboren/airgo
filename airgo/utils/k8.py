from airgo.exceptions import AirgoException


def k8_str_test(str_: str, param_name: str) -> None:
    if str_.startswith("-") or not str_.replace("-", "").isalnum():
        raise AirgoException(
            f"""Parameter {param_name} must consist of alpha-numeric characters or '-',
            and must start with an alpha-numeric character (e.g. My-name1-2, 123-NAME).
            You have '{str_}'
            """
        )


def k8_str_filter(str_: str) -> str:
    return str_.replace("_", "-")
