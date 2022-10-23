from collections import defaultdict


def defaultify(d):
    if isinstance(d, dict):
        return defaultdict(lambda: None, {k: defaultify(v) for k, v in d.items()})
    elif isinstance(d, list):
        return [defaultify(e) for e in d]
    else:
        return d