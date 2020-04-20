# Problem 2:
#
# Sort String Using Given Function
#
# Problem Description:
#
# Given a string and given function which return only
# true or false, return a sorted string using only the
# given function.
#
# Given functions:
#
# public boolean isA() {
# return true
# }
#
# public boolean isB() {
# return true
# }
#
# public boolean isC() {
# return true
# }
#
# Input String: AABCABBA
# Expected Output: AAAABBBC
##########################################################
def isA(c):
   return (c == 'a') or (c == 'A')

def isB(c):
   return (c == 'b') or (c == 'B')

def isC(c):
   return (c == 'c') or (c == 'C')

def sort_string(s1):
    if s1 is None:
        return None

    ast = ''
    bst = ''
    cst = ''

    for c in s1:
        if isA(c):
            ast += c
        elif isB(c):
            bst += c
        elif isC(c):
            cst += c

    return (ast + bst + cst)
#############################################################
s1 = 'AABCABBA'
print ('input: ', s1)
print('output: ', sort_string(s1))
###################################################################
# Test run:
# Rajendras-MacBook-Air:hacker rajendrakumar$ python3 nigel_prob2.py
# input:  AABCABBA
# output:  AAAABBBC
####################################################################