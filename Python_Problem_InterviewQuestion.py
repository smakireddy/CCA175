# Enter your code here. Read input from STDIN. Print output to STDOUT


# Given an array of charactors, sort the elements to make same charactors not occur consecutively in the array

# ['a', 'a', 'a', 'a', 'b', 'b', 'b']
# ->  ['a', 'b', 'a', 'b', 'a', 'b', 'a']

# ['a', 'a', 'b', 'b', 'c', 'c']
# ->  ['a', 'b', 'c', 'a', 'b', 'c']


# ['a', 'a']
# -> invalid, throw a RuntimeException

# example #1
# [a, a, b] -> [a, b, a]
# -> { a -> 2, b -> 1}
# -> a, b, a

# [a, a, b, b]
# -> {a -> 0, b -> 0}
# -> a, b, a, b

# [a, b, c, c, c]
# -> {a->0, b->0, c->0}
# -> c, a, c, b, c

def newarr (arr):
    mydict = dict([[x,arr.count(x)] for x in set(arr)])
    count = sum(mydict.values())
    result = []
    
    for i in range(1,count+1):
        print i
        max_key = max(mydict, key=mydict.get)
        print max_key
        result.append(max_key)
        mydict[max_key]-=1    
    
    print result
    
    return mydict    

a = ['a', 'b', 'c', 'c']

print(newarr(a))


# [a,a,b,b,c,c] -> [a,b,c,a,b,c]  [b,c,a,c,a,b]


