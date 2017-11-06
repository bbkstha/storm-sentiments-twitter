
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;


public class TopologyGenerator {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter", new TwitterStreammingSpout(),1);
        builder.setBolt("language", new LanguageDetectorBolt(), 1).shuffleGrouping("twitter");
        builder.setBolt("logger", new LoggerBolt(), 1).shuffleGrouping("language");
        //builder.setBolt("sentiment", new SentimentBolt(), 1).shuffleGrouping("logger");


        Config conf = new Config();
        conf.setDebug(false);

        conf.setMaxTaskParallelism(3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("storm-sentiments-twitter", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.shutdown();
    }


}
