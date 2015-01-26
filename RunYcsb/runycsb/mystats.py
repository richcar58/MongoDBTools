from math import sqrt

def mean(lst):
    """calculates mean"""
    return sum(lst) / len(lst)

def stddev(lst):
    """returns the sample standard deviation of lst"""
    if len(lst) <= 1:
        return 0
    mn = mean(lst)
    variance = (sum([(e-mn)**2 for e in lst]))/(len(lst)-1)
    return sqrt(variance)

# Print the SAMPLE standard deviation for the example 
# used in the statistic.py module of python 3 to show
# that we perform the same calculation.  
if __name__ == '__main__':
    lst = [1.5, 2.5, 2.5, 2.75, 3.25, 4.75]
    print(lst)
    std = stddev(lst)
    print(std)
    assert(1.0810874155219827 == std)
