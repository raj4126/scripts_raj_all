B = list(input())
cnt = 0
for i in range(len(B)):
    if B[i: i + 3] == ['0', '1', '0']:
        B[i + 2] = '1'
        cnt += 1
print(cnt)

