# Problem:  given a below binary tree exercise of integers to you to find the minimum of each
# level in the tree and return it as list.
#
#
#   1
# /   \
# 3    5
# / \ / \
# 9 8 4 7
#
# [1, 3, 4]
#---------------------------------------------------------------------------------------
# Algorithm:
# 1. do a level order traversal of the binary tree using a queue and breath first search
# 2. at each level, get all the node data, find the minimum and store in an array
# 3. add all children of current level modes into the queue and go to step 1 until done
#---------------------------------------------------------------------------------------
#class to create a new node
class Node:
    def __init__(self, data):
        self.data = data
        self.left = None
        self.right = None
#---------------------------------------------------------------------------------------
# Find minimum of each level of a Binary Tree iteratively using breath first search
def get_per_level_minimum(root, level_min_results):
    if (root is None):
        return []
    # create a queue for a binary search traversal
    q = []  # enqueue is q.append(data), dequeue is q.pop(0)
    q.append(root)
    while(len(q) != 0):
        level_data_min = get_level_data_min(q) #get data of all nodes and find minimum
        level_min_results.append(level_data_min) #save minimum data at each level
        q = add_children(q) #get children of current nodes and continue

def get_level_data_min(q):
    #return the minimum of a level data
    if (q is None):
        return None
    data = []
    for node in q:
        data.append(node.data)
    return min(data)

def add_children(q):
    # return all children of current level nodes in q
    if (q is None):
        return []
    q_children = []
    for node in q:
        if node.left is not None:
          q_children.append(node.left)
        if node.right is not None:
          q_children.append(node.right)
    return q_children
 #----------------------------------------------------------------------------------
# Driver Code
if __name__ == "__main__":
    #build the input tree
    root = Node(1)
    root.left = Node(3)
    root.right = Node(5)
    root.left.left = Node(9)
    root.left.right = Node(8)
    root.right.left = Node(4)
    root.right.right = Node(7)

    print("input data:\n", root.data, "\n", root.left.data, root.right.data, "\n",
          root.left.left.data, root.left.right.data,
          root.right.left.data, root.right.right.data)
    #get minimum data at each level using breath first search
    level_min_results = []
    get_per_level_minimum(root, level_min_results)
    print("level_min_results:\n ", level_min_results)
#-------------------------------------------------------------------------------------
#output:

# python3 per_level_min_bst.py
# input data:
# 1
# 3 5
# 9 8 4 7
# level_min_results:
# [1, 3, 4]
#---------------------------------------------------------------------------------------
