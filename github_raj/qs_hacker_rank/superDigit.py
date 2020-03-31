def superDigit(n):
  if (n == 0) or (n == None):
      return 0

  sum = 0
  while n > 9:
      r = n % 10
      sum = sum + r
      n = (n - r)/10

  sum = sum + n

  return sum











#---------------------------------
n = 123499
print(superDigit(n))