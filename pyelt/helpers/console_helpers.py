# class ConsoleColors:
#     PURPLE = '\033[95m'
#     CYAN = '\033[96m'
#     DARKCYAN = '\033[36m'
#     BLUE = '\033[94m'
#     GREEN = '\033[92m'
#     YELLOW = '\033[93m'
#     GRAY = '\033[91m'
#     LIGHTGRAY = '\033[37m'
#     RED = '\033[91m'
#     FILLED_RED = '\033[101m'
#     BOLD = '\033[1m'
#     UNDERLINE = '\033[4m'
#     END = '\033[0m'
#
#
# def pprint(value):
#     """Maakt het mogelijk in kleur te printen in de console
#     Gebruik html-achtige tags zoals: <red>rood</>
#     """
#     if isinstance(value, list):
#         for val in value:
#             pprint(val)
#     elif isinstance(value, str):
#         value = value.replace('<br>', '\r\n')
#         value = value.replace('<\br>', '\r\n')
#         value = value.replace('<b>', ConsoleColors.BOLD)
#         value = value.replace('<u>', ConsoleColors.UNDERLINE)
#         value = value.replace('<red>', ConsoleColors.RED)
#         value = value.replace('<gray>', ConsoleColors.GRAY)
#         value = value.replace('<lightgray>', ConsoleColors.LIGHTGRAY)
#         value = value.replace('<yellow>', ConsoleColors.YELLOW)
#         value = value.replace('<blue>', ConsoleColors.BLUE)
#         value = value.replace('<green>', ConsoleColors.GREEN)
#         value = value.replace('<purle>', ConsoleColors.PURPLE)
#         value = value.replace('<cyan>', ConsoleColors.CYAN)
#         value = value.replace('<darkcyan>', ConsoleColors.DARKCYAN)
#         value = value.replace('<filledred>', ConsoleColors.FILLED_RED)
#         value = value.replace('</>', ConsoleColors.END)
#         print(value)
#     else:
#         print(value)
#
# # s = 'Dit is zwarte tekst. <red>Dit is rode en <b><red>dit</><red> is vet.</> Nu weer zwart.'
# # pprint(s)
#
# def test():
#     for i in range(120):
#         s = '\033[{}mDIT IS FORMAT. \033[0mDit niet.'.format(i)
#         print(i, s)
#
#         # test()
