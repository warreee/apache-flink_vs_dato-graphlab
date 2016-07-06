import config
import graphlab as gl

small = config.getSmall()
medium = config.getMedium()
large = config.getLarge()


def transform_data(path_to_file):
    formattedLines = list()
    with open(path_to_file) as f:
        content = f.readlines()
        for l in content:
            l = l.rstrip('\n')
            line = l.split("\t")
            if len(line) > 1:
                i = 1
                while i < len(line):
                    formattedLines.append((line[0] + "\t" + line[i]))
                    i += 1

    f.close()

    with open(path_to_file.replace(".txt", "") + ".formatted.txt", 'w') as f:

        f.write('\n'.join(formattedLines))

transform_data(small)
transform_data(medium)
transform_data(large)

googleVertices = set()
google = gl.SFrame.read_csv(config.getGoogle(), delimiter='\t', header=False)


for i in google['X1']:
    googleVertices.add(str(i))

for i in google['X2']:
    googleVertices.add(str(i))

with open(config.getDataPath() + "googleVertices.txt", 'w') as f:
    f.write('\n'.join(googleVertices))
    
stanfordVertices = set()
stanford = gl.SFrame.read_csv(config.getStanford(), delimiter='\t', header=False)


for i in stanford['X1']:
    stanfordVertices.add(str(i))

for i in stanford['X2']:
    stanfordVertices.add(str(i))

with open(config.getDataPath() + "stanfordVertices.txt", 'w') as f:
    f.write('\n'.join(stanfordVertices))

small = gl.SFrame.read_csv(config.getDataPath() + "sample-small.formatted.txt", delimiter='\t', header=False)
smallVertices = set()

for i in small['X1']:
    smallVertices.add(str(i))

for i in small['X2']:
    smallVertices.add(str(i))

with open(config.getDataPath() + "smallVertices.txt", 'w') as f:
    f.write('\n'.join(smallVertices))
    

medium = gl.SFrame.read_csv(config.getDataPath() + "sample-medium.formatted.txt", delimiter='\t', header=False)
mediumVertices = set()

for i in medium['X1']:
    mediumVertices.add(str(i))

for i in medium['X2']:
    mediumVertices.add(str(i))

with open(config.getDataPath() + "mediumVertices.txt", 'w') as f:
    f.write('\n'.join(mediumVertices))
    

large = gl.SFrame.read_csv(config.getDataPath() + "sample-large.formatted.txt", delimiter='\t', header=False)
largeVertices = set()

for i in large['X1']:
    largeVertices.add(str(i))

for i in large['X2']:
    largeVertices.add(str(i))

with open(config.getDataPath() + "largeVertices.txt", 'w') as f:
    f.write('\n'.join(largeVertices))