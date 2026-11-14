"""A durable local-resource wait; this never authorizes another model request."""


class PostprocessingResourceWait(Exception):
    """The saved answer is retained and the local component can resume later."""
