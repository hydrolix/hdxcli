from typing import Any, Optional, Tuple, Sequence
from copy import deepcopy

__all__ = ['choose_interactively',
           'choose_from_elements_interactively']

_SHOW_MAX_ITEMS = 10


def choose_interactively(prompt, *,
                         default: Optional[str] = None,
                         valid_choices: Optional[Sequence[Any]] = None) -> str:
    choice = None
    if valid_choices and not isinstance(valid_choices, str):
        valid_choices = list(map(str, valid_choices))
    while not choice or choice not in valid_choices:
        choice = input(prompt)
        if choice == '' and default:
            return default
        if not valid_choices:
            return choice
        if choice in valid_choices:
            return choice
        valid_choices_failed = deepcopy(valid_choices)
        for conv_type in (int, float):
            try:
                valid_choices_failed = list(map(str, sorted(conv_type(x) for x in valid_choices_failed)))
                break
            except ValueError:
                pass
        valid_choices_str = (', '.join(valid_choices_failed)
                             if len(valid_choices_failed) < _SHOW_MAX_ITEMS
                             else
                             ', '.join(valid_choices_failed[0:_SHOW_MAX_ITEMS // 2]) + '...' +
                             ', '.join(valid_choices_failed[(-_SHOW_MAX_ITEMS - 1) // 2:-1]) + '. ' +
                             f'({len(valid_choices_failed)} choices in total)')
        print(f'Invalid choice {choice}. Valid values are {valid_choices_str}')


def choose_from_elements_interactively(elements: Sequence[Any]) -> Tuple[int, str]:
    """Choose an index from a list of choices and return the index based on 0-offset.
    and the field name
    """
    for i, a_field in enumerate(elements, 1):
        print(f'{i}. {a_field}')
    print()
    element_index = int(choose_interactively(
        'Please choose an index from the list: ',
        valid_choices=[str(x + 1) for x in range(len(elements))]))
    return element_index - 1, elements[element_index - 1]
