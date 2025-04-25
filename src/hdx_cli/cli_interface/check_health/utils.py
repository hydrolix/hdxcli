import operator


def print_header(header_text: str = "", underline_char: str = "="):
    """print text with a character underlining it"""
    underline = len(header_text) * underline_char
    print(f"\n{header_text}\n{underline}")


def sort_by_field(
    unsorted_list: list = [], field_name: str = "id", reverse: bool = True
):
    """Sort a list of dictionaries by their modified property"""
    return sorted(unsorted_list, key=operator.itemgetter(field_name), reverse=reverse)
