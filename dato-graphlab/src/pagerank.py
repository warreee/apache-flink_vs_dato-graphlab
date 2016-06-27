import graphlab as gl
import config

data = gl.SFrame.read_csv(config.getSmallFormatted(), delimiter=",")

g = gl.graphlab.SGraph()

g = g.add_edges(data, src_field='src', dst_field='dst')


pr = gl.graphlab.pagerank.create(g, max_iterations=100)

scores = pr.get('pagerank').remove_column('delta').sort('pagerank', ascending=False)

scores.save(config.getOutputFolder() + 'out.csv', format='csv')

ranking = list()

for item in scores['__id']:
    ranking.append(item)

