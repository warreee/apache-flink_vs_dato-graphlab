
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;


public class DeltaPageRank {

    public static void main(String[] args) throws Exception {

        int numIterations = 100;
        long numVertices = 875713;

        double threshold = 0.0001 / numVertices;
        double dampeningFactor = 0.85;

        String adjacencyPath = "/home/warreee/projects/apache-flink_vs_dato-graphlab/data/web-Google.txt";
        String outpath = "/home/warreee/projects/apache-flink_vs_dato-graphlab/results/googleDelta";


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Long, long[]>> adjacency = env.readTextFile(adjacencyPath).map(new AdjacencyBuilder());


        DataSet<Tuple2<Long, Double>> initialRanks = adjacency
                .flatMap(new InitialMessageBuilder(numVertices, dampeningFactor))
                .groupBy(0)
                .reduceGroup(new Agg());

        DataSet<Tuple2<Long, Double>> initialDeltas = initialRanks.map(new InitialDeltaBuilder(numVertices));


        // ---------- iterative part ---------

        DeltaIteration<Tuple2<Long, Double>, Tuple2<Long, Double>> adaptiveIteration = initialRanks.iterateDelta(initialDeltas, numIterations, 0);

        DataSet<Tuple2<Long, Double>> deltas = adaptiveIteration.getWorkset()
                .join(adjacency).where(0).equalTo(0).with(new DeltaDistributor(0.85))
                .groupBy(0)
                .reduceGroup(new AggAndFilter(threshold));

        DataSet<Tuple2<Long, Double>> rankUpdates = adaptiveIteration.getSolutionSet()
                .join(deltas).where(0).equalTo(0).with(new SolutionJoin());

        adaptiveIteration.closeWith(rankUpdates, deltas)
                .writeAsCsv(outpath, WriteMode.OVERWRITE);


//		System.out.println(env.getExecutionPlan());

        long start = System.currentTimeMillis();
        env.execute("Adaptive Page Rank");
        long stop = System.currentTimeMillis();
        long elapsed = stop - start;
        System.out.println(elapsed);
    }


    public static final class AdjacencyBuilder implements MapFunction<String, Tuple2<Long, long[]>> {

        @Override
        public Tuple2<Long, long[]> map(String value) throws Exception {
            String[] parts1 = value.split("\t");
            String[] parts2 = value.split(",");
            String parts[];
            if (parts1.length < parts2.length) {
                parts = parts2;
            } else {
                parts = parts1;
            }

            if (parts.length < 1) {
                throw new Exception("Malformed line: " + value);
            }

            long id = Long.parseLong(parts[0]);
            long[] targets = new long[parts.length - 1];
            for (int i = 0; i < targets.length; i++) {
                targets[i] = Long.parseLong(parts[i + 1]);
            }

            return new Tuple2<Long, long[]>(id, targets);
        }
    }

    public static final class InitialMessageBuilder implements FlatMapFunction<Tuple2<Long, long[]>, Tuple2<Long, Double>> {

        private final double initialRank;
        private final double dampeningFactor;
        private final double randomJump;

        public InitialMessageBuilder(double numVertices, double dampeningFactor) {
            this.initialRank = 1.0 / numVertices;
            this.dampeningFactor = dampeningFactor;
            this.randomJump = (1.0 - dampeningFactor) / numVertices;
        }

        @Override
        public void flatMap(Tuple2<Long, long[]> value, Collector<Tuple2<Long, Double>> out) {
            long[] targets = value.f1;
            double rankPerTarget = initialRank * dampeningFactor / targets.length;

            // dampend fraction to targets
            for (long target : targets) {
                out.collect(new Tuple2<Long, Double>(target, rankPerTarget));
            }

            // random jump to self
            out.collect(new Tuple2<Long, Double>(value.f0, randomJump));
        }
    }

    public static final class InitialDeltaBuilder implements MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

        private final double uniformRank;

        public InitialDeltaBuilder(long numVertices) {
            this.uniformRank = 1.0 / numVertices;
        }

        @Override
        public Tuple2<Long, Double> map(Tuple2<Long, Double> initialRank) {
            initialRank.f1 -= uniformRank;
            return initialRank;
        }
    }


    public static final class DeltaDistributor implements FlatJoinFunction<Tuple2<Long, Double>, Tuple2<Long, long[]>, Tuple2<Long, Double>> {

        private final Tuple2<Long, Double> tuple = new Tuple2<Long, Double>();

        private final double dampeningFactor;

        public DeltaDistributor(double dampeningFactor) {
            this.dampeningFactor = dampeningFactor;
        }

        @Override
        public void join(Tuple2<Long, Double> deltaFromPage, Tuple2<Long, long[]> neighbors, Collector<Tuple2<Long, Double>> out) {
            long[] targets = neighbors.f1;

            double deltaPerTarget = dampeningFactor * deltaFromPage.f1 / targets.length;

            tuple.f1 = deltaPerTarget;
            for (long target : targets) {
                tuple.f0 = target;
                out.collect(tuple);
            }
        }
    }


    public static final class Agg implements GroupReduceFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

        @Override
        public void reduce(Iterable<Tuple2<Long, Double>> values, Collector<Tuple2<Long, Double>> out) {
            Long key = null;
            double delta = 0.0;

            for (Tuple2<Long, Double> t : values) {
                key = t.f0;
                delta += t.f1;
            }

            out.collect(new Tuple2<Long, Double>(key, delta));
        }
    }

    public static final class AggAndFilter implements GroupReduceFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

        private final double threshold;

        public AggAndFilter(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void reduce(Iterable<Tuple2<Long, Double>> values, Collector<Tuple2<Long, Double>> out) {
            Long key = null;
            double delta = 0.0;

            for (Tuple2<Long, Double> t : values) {
                key = t.f0;
                delta += t.f1;
            }

            if (Math.abs(delta) > threshold) {
                out.collect(new Tuple2<Long, Double>(key, delta));
            }
        }
    }

    public static final class SolutionJoin implements JoinFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {

        @Override
        public Tuple2<Long, Double> join(Tuple2<Long, Double> page, Tuple2<Long, Double> delta) {
            page.f1 += delta.f1;
            return page;
        }
    }
}