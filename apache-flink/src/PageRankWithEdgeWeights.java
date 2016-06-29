/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.*;
import org.apache.flink.graph.library.PageRank;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.util.Collector;

/**
 * The edges input file is expected to contain one edge per line, with String IDs and double
 * values in the following format:"<sourceVertexID>\t<targetVertexID>\t<edgeValue>".
 * <p>
 * This class is used to create a graph from the input data and then to run a PageRankAlgorithm
 * (present in Flink-gelly graph-library)over it. The algorithm used is a simplified implementation
 * of the actual algorithm; its limitation is that all the pages need to have at least one incoming
 * and one outgoing link for correct results. The vertex-centric algorithm takes as input parameters
 * dampening factor and number of iterations.
 */
public class PageRankWithEdgeWeights {

    private static final double DAMPENING_FACTOR = 0.85;

    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {

        String edgeInputPath = "/software/flink/web-Google.txt";
        int maxIterations = 100;
        String outputPath = "/software/flink/out";


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> linkStrings = env.readTextFile(edgeInputPath);

        DataSet<Edge<String, Double>> edges = linkStrings.flatMap(new AddEdges());

        Graph<String, Double, Double> graph = Graph.fromDataSet(edges,
                new MapFunction<String, Double>() {
                    public Double map(String value) throws Exception {
                        return 1.0;
                    }
                }, env);

        //read the Edge DataSet from the input file
        DataSet<Edge<String, Double>> links = env.readCsvFile(edgeInputPath)
                .fieldDelimiter(",")
                .lineDelimiter("\n")
                .types(String.class, String.class, Double.class)
                .map(new Tuple3ToEdgeMap<String, Double>());


        //create a Graph with vertex values initialized to 1.0
        Graph<String, Double, Double> network = Graph.fromDataSet(links,
                new MapFunction<String, Double>() {
                    public Double map(String value) throws Exception {
                        return 1.0;
                    }
                }, env);


        //for each vertex calculate the total weight of its outgoing edges
        DataSet<Tuple2<String, Double>> sumEdgeWeights =
                graph.reduceOnEdges(new SumWeight(), EdgeDirection.OUT);


        // assign the transition probabilities as edge weights:
        //divide edge weight by the total weight of outgoing edges for that source
        Graph<String, Double, Double> networkWithWeights = graph
                .joinWithEdgesOnSource(sumEdgeWeights,
                        new EdgeJoinFunction<Double, Double>() {
                            @Override
                            public Double edgeJoin(Double v1, Double v2) throws Exception {
                                return v1 / v2;
                            }
                        });

        //Now run the Page Rank algorithm over the weighted graph
        DataSet<Vertex<String, Double>> pageRanks = networkWithWeights.run(
                new PageRank<String>(DAMPENING_FACTOR, maxIterations));

        pageRanks.writeAsCsv(outputPath, "\n", ",", FileSystem.WriteMode.OVERWRITE);
        // since file sinks are lazy,trigger the execution explicitly

        env.setParallelism(6);
        long start = System.currentTimeMillis();
        env.execute("PageRank with Edge Weights");
        long stop = System.currentTimeMillis();
        long elapsed = stop - start;
        System.out.println(elapsed);

    }

    //function to calculate the total weight of outgoing edges from a node
    @SuppressWarnings("serial")
    static final class SumWeight implements ReduceEdgesFunction<Double> {
        @Override
        public Double reduceEdges(Double firstEdgeValue, Double secondEdgeValue) {
            return firstEdgeValue + secondEdgeValue;
        }
    }

    public static class AddEdges implements FlatMapFunction<String, Edge<String, Double>> {
        int i = 0;
        @Override
        public void flatMap(String s, Collector<Edge<String, Double>> collector) throws Exception {

            String[] vertexes = s.split("\t");
            if (vertexes.length > 1) {
                String startVertex = vertexes[0];
                for (int i = 1; i < vertexes.length; i++) {
                    Edge<String, Double> edge = new Edge<>();
                    edge.f0 = startVertex;
                    edge.f1 = vertexes[i];
                    edge.f2 = 100.0;
                    collector.collect(edge);
                }
                System.out.printf("");
            }
        }
    }

}
