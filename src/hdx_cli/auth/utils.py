def chain_calls_ignore_exc(*funcs, **kwargs):
    """
    Chain calls and return on_error_return on failure. Exceptions are considered failure if
    they derived from provided kwarg 'exctype', otherwise they will escape.
    on_error_default kwarg is what is returned in case of failure.

    Function parameters to funcs[0] are provided in kwargs, and subsequent results
    are provided as input of the first function.
    """
    # Setup function
    exctype = kwargs.get("exctype", Exception)
    on_error_return = kwargs.get("on_error_return", None)
    try:
        del kwargs["exctype"]
    except:  # pylint:disable=bare-except
        pass
    try:
        del kwargs["on_error_return"]
    except:  # pylint:disable=bare-except
        pass

    # Run functions
    try:
        result = funcs[0](**kwargs)
        if len(funcs) > 1:
            for func in funcs[1:]:
                result = func(result)
        return result
    except exctype:
        return on_error_return
