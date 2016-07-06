import graphlab as gl
import config

small = gl.SFrame.read_csv(config.getDataPath() + "sample-small.formatted.txt", delimiter="\t", header=False)
medium = gl.SFrame.read_csv(config.getDataPath() + "sample-medium.formatted.txt", delimiter="\t", header=False)
large = gl.SFrame.read_csv(config.getDataPath() + "sample-large.formatted.txt", delimiter="\t", header=False)
#stanford = gl.SFrame.read_csv(config.getStanford(), delimiter="\t", header=False)
#google = gl.SFrame.read_csv(config.getGoogle(), delimiter="\t", header=False)

data = {'small': small, 'medium': medium, 'large': large}

for d in data.keys():

    g = gl.graphlab.SGraph()

    g = g.add_edges(data.get(d), src_field='X1', dst_field='X2')

    pr = gl.graphlab.pagerank.create(g, max_iterations=100)

    scores = pr.get('pagerank').remove_column('delta').sort('pagerank', ascending=False)

    name = config.getOutputFolder() + d + '.csv'

    scores.save(name, format='csv')

    with open(name, 'r') as fin:
        headerData = fin.read().splitlines(True)
    with open(name, 'w') as fout:
        fout.writelines(headerData[1:])
