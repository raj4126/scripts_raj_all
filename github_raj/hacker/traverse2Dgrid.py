#Q:  Print path from a start node to end node in a 2D gird.

grid = [[1,1,0,1],
        [1,1,0,0],
        [0,1,1,1],
        [0,1,0,1]]
v = [
    [0,0,0,0],
    [0,0,0,0],
    [0,0,0,0],
    [0,0,0,0]
     ]
s = grid[0][0]
e = grid[3][3]
path = []

def getNeighbors(grid, tup):
    sx = tup[0]
    sy = tup[1]
    qq = []
    if sx != 0:
       lx = sx - 1
       if v[lx][sy] != 1 and (grid[lx][sy] == 1):
         qq.append((lx, sy)) #left

    if sx != 3:
       rx = sx + 1
       if v[sx][rx] != 1 and (grid[rx][sy] == 1):
         qq.append((rx, sy)) # right

    if sy != 0:
       ty = sy - 1
       if (v[sx][ty] != 1) and (grid[sx][ty] == 1):
         qq.append((sx, ty))  #top

    if sy != 3:
        by = sy + 1
        if v[sx][by] != 1 and (grid[sx][by] == 1):
          qq.append((sx, by))  #bottom

    return qq

q = []
q.append((0,0))

def tracepath(grid, stup, etup, v, path):
    sx = stup[0]
    sy = stup[1]
    ex = etup[0]
    ey = etup[1]
    q = []
    q.append((sx,sy))
    while len(q) > 0:
        print(q)
        (sx, sy) = q.pop(0)
        path.append((sx,sy))
        v[sx][sy] = 1
        print('sx,sy,ex,ey:  ', sx, sy, ex, ey)
        if (sx, sy) == (ex, ey):
          print('path found')
          return path

        neighbors = getNeighbors(grid, (sx,sy))
        for neighbor in neighbors:
            if v[neighbor[0]][neighbor[1]] != 1:
               q.append((neighbor[0], neighbor[1]))
    return path

#-----------------
path = tracepath(grid, (0,0), (3,1), v, path)
print('path: ', path)
