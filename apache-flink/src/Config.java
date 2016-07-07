public class Config {

    public static String getDataPath() {
        return "/home/warreee/projects/apache-flink_vs_dato-graphlab/data/";
    }

    public static String getSmallFormatted() {
        return getDataPath() + "sample-small.formatted.txt";
    }

    public static String getGoogle() {
        return getDataPath() + "web-Google.txt";
    }


    public static String getOutputPath() {
        return "/home/warreee/projects/apache-flink_vs_dato-graphlab/results/";
    }
}
