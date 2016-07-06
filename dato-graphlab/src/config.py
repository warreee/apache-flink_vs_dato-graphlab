import os


def getDataPath():
    return os.getcwd().replace("dato-graphlab/src", "data/")


def getSmall():
    return getDataPath() + "sample-small.txt"


def getMedium():
    return getDataPath() + "sample-medium.txt"


def getLarge():
    return getDataPath() + "sample-large.txt"

def getGoogle():
    return getDataPath() + "web-Google.txt"


def getSmallFormatted():
    return getDataPath() + "sample-small.formatted.txt"


def getOutputFolder():
    return os.getcwd().replace("dato-graphlab/src", "results/")


