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

def parse_taxon_id(raw):
    if raw is None:
        return None

    # This is to ensure that a bool doesn't succeed, since int(True) = 1
    if isinstance(raw, int) and not isinstance(raw, bool):
        return raw

    try:
        return int(raw)
    except (TypeError, ValueError):
        pass

    try:
        f = float(raw)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid taxon id: {raw}")

    if not f.is_integer():
        raise ValueError(f"Taxon id must be a whole number: {raw}")

    return int(f)