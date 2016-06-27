import graphlab as gl
import config


def compare(correctList, newList, scope):
    """
    A method to compare two list which are not necessarily the same but should be regarded as the same.
    The 2 input lists are supposed to have the same items and the same length, also no duplicates are allowed.
    Parameters
    ----------
    correctList: the list where the new list is compared to
    newList: the list which is compared to the correct list
    scope: the number of places before and behind an item

    Returns
    -------
    A double which represent the percentage of comparison.
    """
    correct = 0
    if len(correctList) != len(newList):
        raise ValueError("List must be of equal size")
    else:
        for item in newList:
            low = correctList.index(item) - scope
            high = correctList.index(item) + scope

            if low < 0:
                low = 0
            if high > len(correctList) - 1:
                high = len(correctList) - 1

            if low <= newList.index(item) <= high:
                correct += 1

        return float(correct) / len(correctList)


newList = list()
new = gl.SFrame.read_csv(config.getOutputFolder() + "out.csv")['__id']
for i in new:
    newList.append(i)

correctList = list()
correct = \
    gl.SFrame.read_csv(config.getOutputFolder() + "sample-small.pagerank.txt", delimiter='\t', header=False,
                       skiprows=6)[
        'X2']
for i in correct:
    correctList.append(i)

newList2 = list()
new2 = \
    gl.SFrame.read_csv(config.getOutputFolder() + "out2.csv", delimiter=',', header=False).sort('X2', ascending=False)[
        'X1']
for i in new2:
    newList2.append(i)

print compare(correctList, newList2, 5)
