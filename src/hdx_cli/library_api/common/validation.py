def is_valid_scheme(scheme):
    return scheme in ("https", "http")


def is_valid_username(username):
    return not username[0].isdigit()


def is_valid_hostname(hostname):
    import re  # pylint:disable=import-outside-toplevel

    if not hostname or len(hostname) > 255:
        return False
    if hostname[-1] == ".":
        hostname = hostname[:-1]  # strip exactly one dot from the right, if present
    pattern = r"^([\w\d][\w\d\.\-]+[\w\d])(\:([1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5]))?$"
    allowed = re.compile(pattern, re.IGNORECASE)
    return all(allowed.match(x) for x in hostname.split("."))
