import config
import graphlab as gl

small = config.getSmall()
medium = config.getMedium()
large = config.getLarge()


def transform_data(path_to_file):
    formattedLines = list()
    with open(path_to_file) as f:
        formattedLines.append("src,dst")
        content = f.readlines()
        for l in content:
            l = l.rstrip('\n')
            line = l.split("\t")
            if len(line) > 1:
                i = 1
                while i < len(line):
                    formattedLines.append((line[0] + "," + line[i]))
                    i += 1

    f.close()

    with open(path_to_file.replace(".txt", "") + ".formatted.txt", 'w') as f:

        f.write('\n'.join(formattedLines))


vertices = set()


google = gl.SFrame.read_csv(config.getGoogle(), delimiter='\t', header=False)


for i in google['X1']:
    vertices.add(str(i))

for i in google['X2']:
    vertices.add(str(i))

with open(config.getDataPath() + "googleVertices.txt", 'w') as f:
    f.write('\n'.join(vertices))

small = gl.SFrame.read_csv(config.getSmallFormatted(), delimiter=',', header=False)
smallVertices = set()

for i in small['X1']:
    smallVertices.add(str(i))

for i in small['X2']:
    smallVertices.add(str(i))

with open(config.getDataPath() + "smallVertices.txt", 'w') as f:
    f.write('\n'.join(smallVertices))