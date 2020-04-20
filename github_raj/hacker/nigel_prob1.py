# Problem 1:
# Compress String Problem
#
# Problem Description:
#
# Given a string, retrun a compressed string with number
# of times a character is repeated only if the
# character is repeated more than three times consecutively.
#
# Input1:
# uuuuuuuuuuuu
#
# Output1:
# 12Xu
#
# Input2:
# abbccccuiiiiii
#
# Output2:
# abb4Xcu6Xi
####################################################################
def parse(s1):
    d = {}
    for c in s1:
        ct = d.get(c, 0)
        d[c] = ct + 1
    return d

def csp(s1):
    if s1 is None:
        return None

    parsed_str = ''
    d = parse(s1)
    seen = set()
    for c in s1:
        if (c in seen) and (d[c] > 3):
            continue
        count = d.get(c, 0)
        seen.add(c)
        if count > 3:
            parsed_str += c + 'X' + str(count)
        else:
            parsed_str += c

    return parsed_str
####################################################################
s1 = 'uuuummpppppuu'
print('input:  ', s1)
print('output: ', csp(s1))
####################################################################
#Test run:
# Rajendras-MacBook-Air:hacker rajendrakumar$ python3 nigel_prob1.py
# input:   uuuummpppppuu
# output:  uX6mmpX5
#####################################################################