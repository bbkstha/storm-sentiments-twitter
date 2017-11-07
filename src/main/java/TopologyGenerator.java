
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import resources.RollingCountingLossyAlg;

import resources.*;


public class TopologyGenerator {

    public static void main(String[] args) throws Exception {

        String runOnCluster=null;
//        if (args.length >2) {
//            runOnCluster = args[1];
//        }


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter", new TwitterStreammingSpout(),1);
        builder.setBolt("language", new LanguageDetectorBolt(), 4).shuffleGrouping("twitter");
        builder.setBolt("sentiment", new SentimentBolt(), 4).shuffleGrouping("language");
        builder.setBolt("name_entity", new NameEntitiyBolt(), 4).shuffleGrouping("sentiment");
        builder.setBolt("hashtag", new HastagBolt(), 4).shuffleGrouping("sentiment");
        //builder.setBolt("name_lossycount", new LossycountBolt(), 1).shuffleGrouping("name_entity");
        //builder.setBolt("hashtag_lossycount", new LossycountBolt(), 1).shuffleGrouping("hashtag");

        //builder.setBolt("name_entity_count", new RollingCountingLossyAlg(), 4).fieldsGrouping("name_entity", new Fields("entity"));
        builder.setBolt("hashtag_count", new RollingCountingLossyAlg(), 4).fieldsGrouping("hashtag", new Fields("entity"));

//        builder.setBolt("name_entity-intermediate-ranking", new IntermediateRankingBolt(100), 4).fieldsGrouping("name_entity_count", new Fields(
//                "entity"));
//        builder.setBolt("name_entity-total-ranking", new TopRankingBolt(100)).globalGrouping("name_entity-intermediate-ranking");
//        builder.setBolt("hashtag-intermediate-ranking", new IntermediateRankingBolt(100), 4).fieldsGrouping("hashtag_count", new Fields(
//                "entity"));
//        builder.setBolt("hashtag-total-ranking", new TopRankingBolt(100)).globalGrouping("hashtag-intermediate-ranking");
//        builder.setBolt("logger1", new LoggerBolt("~/tmp/log1.txt")).shuffleGrouping("name_entity-total-ranking");
//        builder.setBolt("logger2", new LoggerBolt("~/tmp/log2.txt")).shuffleGrouping("hashtag-total-ranking");


        //builder.setBolt("hashtag-ranking-print", new FileWriterBolt("HASHTAG_RANKING.txt")).shuffleGrouping("hashtag-total-ranking");



        Config conf = new Config();
        conf.setDebug(false);

        if (runOnCluster!=null) {
            conf.setNumWorkers(4);

            StormSubmitter.submitTopology("storm-sentiments-twitter", conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("storm-sentiments-twitter", conf, builder.createTopology());

            Utils.sleep(10000); //for testing purpose

            cluster.shutdown();
        }


//        conf.setMaxTaskParallelism(3);
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("storm-sentiments-twitter", conf, builder.createTopology());
        //Utils.sleep(100000);
        //cluster.shutdown();
    }


}
