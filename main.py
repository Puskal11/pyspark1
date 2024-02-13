# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

# Defining functions below:

def sum(x,y,z):
    return x+y+z
def displaysum(x,y,z):
    print(x+y+z)

def display():
    print("Hello World")

def sum1(x,y=20,z=10):
    print(x+y+z)

def display1(a,b,c):
    print(a,b,c)


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    #print_hi('PyCharm')

    #DataType and variables:

    x=5
    y="John"
    print(x ,y)
    x=str(33)
    print(type(x))
    print(x)
    y=int(x)
    print(type(y))
    print(y)

    x = str(3)    # x will be '3'
    y = int(3)    # y will be 3
    z = float(3)  # z will be 3.0
    print(x,y,z)
    print(type(x),type(y),type(z))
    name ="John"
    firtName='Puskal'
    print(type(name),type(firtName))

    a=5
    b='10'
    c=str(a)+b
    print(c)
    x,y,z =1,2,3
    print(x,y,z)
    x=y=z=5
    print(x,y,z)

    #Datatypes

    l=[1,2,3,4,5,6,7,8,9,0]
    print(l)
    print(l[0])
    print(l[1])
    print(l[6])
    l[0]=100
    print(l)

    l=[1,2,3]
    x,y,z=l
    print(x,y,z)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

    print(sum(x,y,z))
    print(displaysum(x,y,z))
    print(display())

    x1=1
    y1=2
    z1=3
    s=sum(x1,y1,z1)
    print(s)
    s =sum(1,2,3)
    print(s)
    s=sum(1,2,z1)
    print(s)
    displaysum(10,20,30)
    display()
    sum1(1,2)
    display1(1,2,3)
    display1(b=2,c=3,a=1)
    sum1(1)

    # Different Operators in Python("==">> equal operator(gives T or F results), >=,=<

    ll=['a','b']
    print('c' not in ll)

    ll=['a','b','c',1,True, False,1]
    print(ll)
    ll=list() #initializing empty list
    ll.append('a')
    ll.append('b')
    ll=['a','b','c',1,True, False,1]

    ll1=list()
    ll1.append(2) #append adds element in the list
    ll1.append(1)
    ll1.append(5)
    print(ll1)

    print(ll[6]) #when indices are starting from LEFT then starts with "0"
    print(ll[-1]) #when indices starts from RIGHT then starts with "-1"
    print(ll[5])
    print(ll[-2])

    ll=['a','b','c',1,True, False,1]
    print(ll[0:3])
    print(ll[1:3])
    print(ll[3:])

    ll.remove('c')
    print(ll)
    ll.pop(0)
    print(ll)

    ll=['a','b','c',1,True, False,1]

    for x in ll:
        print(x,'loop')
    print("range of loop staring from beginning and after 2nd position")
    for i in range(0, len(ll),2):
        print(ll[i])

    print("range of loop staring from 1")
    for i in range(1, len(ll)):
        print(ll[i])


#for loop is good when you want to iterate over all the element in list AND While to execute until somthing false


    a=10
    b=12
    c=10

    if a>b:
        print("a is greater than b")
    elif a==b:
        print("a == b")
    elif a==c:
        print("a == c")
    else:
        print("a is less than b")

    ls=[1,2,3,4,5,6,7,8,9,10]


    for x in ls:
        if x%2==0:
            print(x)

    #1st way
        ls = [5+x for x in ls]
        print(ls)

    #2nd way
        for x in range(0, len(ls),x+5):
            print(ls)


    days=['S','M','W','M','M','F','S']

    d=dict()

    for x in days:
        if x in d:
            d[x]=d[x]+1
        else:
            d[x]=1

    print(d)

    paragraph = "hello world hi world"
    words = paragraph.split(" ")
    #letters= [letter for letter in paragraph]
    print(words)
    #print(letters)

    d1=dict()

    for x in words:
        if x in d1:
            d1[x]=d1[x]+1
        else:
            d1[x]=1

        print(d1)


# Daily python- Write a python code to reverse a String:

    str1= "Aforapple"
    reverse_string = str1[::-1]
    print(reverse_string)





