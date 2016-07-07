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
        return float(correct) / (len(newList) - notfound)


dataSets = ['small', 'medium', 'large']

for d in dataSets:

    newList = list()

    data = gl.SFrame.read_csv(config.getOutputFolder() + d + ".csv", delimiter=",", header=False)




    new = data.groupby(key_columns='X2', operations={'X1': agg.SUM('X1')}).sort('X2', ascending=False)['X1']

    for i in new:
        newList.append(i)

    correctList = list()
    correct = \
        gl.SFrame.read_csv(config.getOutputFolder() + "sample-" + d + ".pagerank.txt", delimiter='\t', header=False,
                           skiprows=6).groupby(key_columns='X1', operations={'X2': agg.SUM('X2')}).sort('X1', ascending=False)[
            'X2']
    for i in correct:
        correctList.append(i)
    print ""


    with open(config.getOutputFolder() + d + "Comparison.txt", 'w') as f:
        f.write(str(0) + ',' + str(compare(correctList, newList, 0)) + '\n')
        f.write(str(1) + ',' + str(compare(correctList, newList, 1)) + '\n')
        f.write(str(2) + ',' + str(compare(correctList, newList, 2)) + '\n')
        f.write(str(3) + ',' + str(compare(correctList, newList, 3)) + '\n')
        f.write(str(4) + ',' + str(compare(correctList, newList, 4)) + '\n')
        f.write(str(5) + ',' + str(compare(correctList, newList, 5)) + '\n')
        f.close()
