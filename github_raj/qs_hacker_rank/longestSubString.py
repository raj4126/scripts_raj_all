def largestSubString(s):
    l = []

    for i in range(len(s)):
        s1 = set()
        smax = 0
        for j in range(i, len(s)):
            smax = smax + 1
            if(s[j]) in s1:
                continue
            else:
                s1.add(s[j])
            if (len(s1) > 2):
               l.append(smax - 1)
               break
    return l


#-----------------------------------------
s = "kpyzyyyyyzyyyhdyx"
l = largestSubString(s)
print(l, max(l))
