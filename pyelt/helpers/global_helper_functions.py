


def camelcase_to_underscores(string):
    """# example = 'CamelCase_Example'
    # example2 = 'CamelCase_Example_AdditionalUnderscore'"""
    string = string[0].lower() + string[1:len(string)]

    for i in string:
        if i.isupper():
            string = string.replace(i, '_' + i)
    string = string.lower().replace('__', '_')

    return string

