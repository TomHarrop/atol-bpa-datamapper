def safe_get(accessor_fn, default=None):
    """
    Safely evaluates a nested attribute/dict chain using a lambda expression.
    
    Parameters:
        accessor_fn (callable): A lambda that accesses the desired property chain.
        default: The fallback value if any attribute in the chain is None.
        
    Returns:
        The result of accessor_fn() if successful, otherwise `default`.
    """
    try:
        return accessor_fn()
    except (AttributeError, KeyError, TypeError):
        return default