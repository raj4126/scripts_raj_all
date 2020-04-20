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
def csp(s1):
    if s1 is None:
        return None

    previous = s1[0]
    ct = 1
    output = ''
    for c in s1[1:]:
      current = c
      if previous == current:
          ct += 1
      elif ct > 3:
        output += previous + 'X' + str(ct)
        ct = 1
        previous = current
      else:
        newstring = ''
        for i in range(ct):
            newstring += previous
        output += newstring
        ct = 1
        previous = current
    if ct > 0:
        for i in range(ct):
            output = output + previous

    return output
####################################################################
s1 = 'uuuummpppppuu'
print('input:  ', s1)
print('output: ', csp(s1))
####################################################################
#Test run:
# Rajendras-MacBook-Air:hacker rajendrakumar$ python3 nigel_prob1_updated.py
# input:   uuuummpppppuu
# output:  uX4mmpX5uu
#####################################################################