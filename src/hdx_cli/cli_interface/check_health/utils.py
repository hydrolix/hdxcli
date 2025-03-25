def print_header(header_text: str = "", underline_char: str = "="):
    """print text with a character underlining it"""
    underline = len(header_text) * underline_char
    print(f"\n{header_text}\n{underline}")
