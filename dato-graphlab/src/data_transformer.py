import config

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
                    formattedLines.append((line[0] + "," + line[i]))
                    i += 1
            else:
                formattedLines.append(line[0])

    f.close()

    with open(path_to_file.replace(".txt", "") + ".formatted.txt", 'w') as f:

        f.write('\n'.join(formattedLines))

transform_data(small)
transform_data(medium)
transform_data(large)
