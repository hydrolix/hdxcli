
def is_valid_username(username):
    return not username[0].isdigit()


def is_valid_hostname(hostname):
    # Credits to https://stackoverflow.com/questions/2532053/validate-a-hostname-string
    # Just import here, since this function is not called often at all
    import re # pylint:disable=import-outside-toplevel

    if len(hostname) > 255:
        return False
    if hostname[-1] == ".":
        hostname = hostname[:-1] # strip exactly one dot from the right, if present
    allowed = re.compile("(?!-)[A-Z\d-]{1,63}(?<!-)$", re.IGNORECASE)
    return all(allowed.match(x) for x in hostname.split("."))
