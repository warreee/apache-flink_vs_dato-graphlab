import graphlab
import config

data = graphlab.SFrame.read_csv(config.getSmallFormatted())

g = graphlab.SGraph()

g = g.add_edges(data, src_field='src', dst_field='dst')

pr = graphlab.pagerank.create(g)

pr_out = pr['pagerank']

g.vertices['pagerank'] = pr['graph'].vertices['pagerank']