import types

def add_to_class(classname):
    """Add decorated function as a method to the whole class *classname*.
    The decorated function should follow a method-like syntax, with the first argument ``self`` that references the class instance.
    The added method is accessible also from subclasses of *classname*.
    """
    def decorator(func):
        setattr(classname, func.__name__, func)
    return decorator


def add_to_instance(instance):
    """Add decorated function as a method to one particular *instance*.
    The decorated function should follow a method-like syntax, with the first argument ``self`` that references the class instance.
    The added method is accessible only for that one particular instance and it overrides any methods with the same name defined on a class level (in original class' source) or added with add_to_class decorator.
    """
    def decorator(func):
        func = types.MethodType(func, instance)
        setattr(instance, func.__func__.__name__, func)
    return decorator
