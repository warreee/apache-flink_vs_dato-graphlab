public class Config {

    public static String getDataPath() {
        return (System.getProperty("user.dir") + "/").replace("/apache-flink/", "/data/");
    }

    public static String getSmallFormatted() {
        return getDataPath() + "sample-small.formatted.txt";
    }


}
