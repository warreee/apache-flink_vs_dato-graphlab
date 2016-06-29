/**
 * This class should allow to reproduce the experiments in a consistent way.
 */
public class Script {

    public static void main(String[] args) throws Exception {
        String[] arguments = new String[] {"123"};

        PageRankWithEdgeWeights.main(arguments);

        /**
         * Setup 1: the small data set
         */

        String[] arguments1 = new String[10];
        String inputPath = Config.getSmallFormatted();


    }

}
