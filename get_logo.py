from time import sleep

s = '\u2586'
a = 'a'
strings = [
    s * 8 + ' ' * 2 + s * 7 + ' ' * 5 + s * 7 + ' ' * 2 + s * 11 + ' ' * 7 + s * 7,
    s * 8 + ' ' * 2 + s * 8 + ' ' * 3 + s * 8 + ' ' * 2 + s * 12 + ' ' * 4 + s * 11,
    ' ' * 2 + s * 4 + ' ' * 6 + s * 7 + ' ' + s * 7 + ' ' * 6 + s * 3 + ' ' * 3 + s * 4 + ' ' * 3 + s * 4 + ' ' * 5 + s * 4,
    ' ' * 2 + s * 4 + ' ' * 6 + s * 15 + ' ' * 6 + s * 8 + ' ' * 4 + s * 4 + s * 11,
    ' ' * 2 + s * 4 + ' ' * 6 + s * 3 + ' ' + s * 7 + ' ' + s * 3 + ' ' * 6 + s * 8 + ' ' * 4 + s * 4 + s * 11,
    ' ' * 2 + s * 4 + ' ' * 6 + s * 3 + ' ' * 2 + s * 5 + ' ' * 2 + s * 3 + ' ' * 6 + s * 3 + ' ' * 3 + s * 4 + ' ' * 2 + s * 4 + ' ' * 7 + s * 4,
    s * 8 + ' ' * 2 + s * 5 + ' ' * 3 + s * 3 + ' ' * 3 + s * 5 + ' ' * 2 + s * 12 + ' ' * 2 + s * 4 + ' ' * 7 + s * 4,
    s * 8 + ' ' * 2 + s * 5 + ' ' * 4 + s + ' ' * 4 + s * 5 + ' ' * 2 + s * 11 + ' ' * 3 + s * 4 + ' ' * 7 + s * 4,
    ' ',
    ' ' + s * 4 + ' ' * 3 + s * 4 + ' ' * 4 + s * 7 + ' ' * 3 + s * 6 + ' ' * 3 + s * 4 + ' ' * 3 + s * 3 + ' ' * 3 + s * 2 + ' ' * 2 + s * 2 + ' ' * 4 + s * 2,
    s * 2 + ' ' * 5 + s * 2 + ' ' * 2 + s * 2 + ' ' * 2 + s * 2 + ' ' * 2 + s + ' ' * 2 + s * 2 + ' ' * 2 + s * 2 + ' ' * 2 + s * 2 + ' ' * 2 + s * 2 + ' ' * 2 + s * 2 + ' ' * 2 + s * 4 + ' ' * 2 + s * 2 + ' ' * 3 + s * 2 + ' ' * 2 + s * 2,
    s * 2 + ' ' * 5 + s * 2 + ' ' * 2 + s * 2 + ' ' * 2 + s * 2 + ' ' * 2 + s + ' ' * 2 + s * 2 + ' ' * 2 + s * 4 + ' ' * 4 + s * 6 + ' ' * 2 + s * 2 + ' ' * 2 + s * 4 + ' ' * 5 + s * 2,
    ' ' + s * 4 + ' ' * 3 + s * 4 + ' ' * 3 + s * 2 + ' ' * 2 + s + ' ' * 2 + s * 2 + ' ' * 2 + s * 2 + ' ' * 6 + s * 2 + ' ' * 2 + s * 2 + ' ' * 2 + s * 2 + ' ' * 4 + s * 2 + ' ' * 5 + s * 2,
]
for string in strings:
    print(string)
    if strings.index(string) < 8:
        sleep(0.05)
    else:
        sleep(0.15)
print('')
