import graphlab as gl
import config
import graphlab.aggregate as agg


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
    notfound = 0
    if len(correctList) < len(newList):
        raise ValueError("Size of newList should be smaller than the correctlist")
    else:
        for item in newList:
            try:
                low = correctList.index(item) - scope
                high = correctList.index(item) + scope

                if low < 0:
                    low = 0
                if high > len(correctList) - 1:
                    high = len(correctList) - 1

                if low <= newList.index(item) <= high:
                    correct += 1
            except ValueError:
                notfound += 1
        print float(correct) / (len(newList) - notfound)
        return float(correct) / (len(newList) - notfound)

newList = list()

data = gl.SFrame.read_csv(config.getOutputFolder() + 'google3.txt', delimiter=" ", header=False).sort('X2',
                                                                                              ascending=False)

new = data.head(25)['X1']

for i in new:
    newList.append(i)

correctList = list()
correct = \
    gl.SFrame.read_csv(config.getOutputFolder() + "google.csv", delimiter=',', header=False,
                       skiprows=6).sort('X2', ascending=False)[
        'X1']
for i in correct:
    correctList.append(i)

with open(config.getOutputFolder() + 'google' + "Comparison.txt", 'w') as f:
    f.write(str(5) + ',' + str(compare(correctList, newList, 5)) + '\n')
    f.write(str(6) + ',' + str(compare(correctList, newList, 6)) + '\n')
    f.write(str(7) + ',' + str(compare(correctList, newList, 7)) + '\n')
    f.write(str(8) + ',' + str(compare(correctList, newList, 8)) + '\n')
    f.write(str(9) + ',' + str(compare(correctList, newList, 9)) + '\n')
    f.write(str(10) + ',' + str(compare(correctList, newList, 10)) + '\n')
    f.close()

newList = list()

data = gl.SFrame.read_csv(config.getOutputFolder() + 'googleEdge.txt', delimiter=",", header=False).filter_by([1.0], 'X2', exclude=True).sort('X2',
                                                                                              ascending=False)
data.print_rows(100)
print correct
new = data.head(25)['X1']

for i in new:
    newList.append(i)


with open(config.getOutputFolder() + 'googleEdge' + "Comparison.txt", 'w') as f:
    f.write(str(5) + ',' + str(compare(correctList, newList, 5)) + '\n')
    f.write(str(6) + ',' + str(compare(correctList, newList, 6)) + '\n')
    f.write(str(7) + ',' + str(compare(correctList, newList, 7)) + '\n')
    f.write(str(8) + ',' + str(compare(correctList, newList, 8)) + '\n')
    f.write(str(9) + ',' + str(compare(correctList, newList, 9)) + '\n')
    f.write(str(10) + ',' + str(compare(correctList, newList, 10)) + '\n')
    f.close()