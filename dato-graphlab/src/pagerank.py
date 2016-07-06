import graphlab as gl
from . import config

data = gl.SFrame.read_csv(config.getSmallFormatted(), delimiter=",", header=False)

g = gl.graphlab.SGraph()

g = g.add_edges(data, src_field='X1', dst_field='X2')


pr = gl.graphlab.pagerank.create(g, max_iterations=100)

scores = pr.get('pagerank').remove_column('delta').sort('pagerank', ascending=False)

scores.save(config.getOutputFolder() + 'out.csv', format='csv')

ranking = list()

for item in scores['__id']:
    ranking.append(item)

