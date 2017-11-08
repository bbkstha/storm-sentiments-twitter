
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;


public class TopologyGenerator {

    public static void main(String[] args) throws Exception {

        String logFile1 = "Top100NameEntity.txt";
        String logFile2 = "Top100Hashtag.txt";
        String runOnCluster=null;
        if (args.length >=3) {
            runOnCluster = args[3];
        }


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter", new TwitterStreammingSpout(),1);
        builder.setBolt("language", new LanguageDetectorBolt(), 4).shuffleGrouping("twitter");
        builder.setBolt("sentiment", new SentimentBolt(), 4).shuffleGrouping("language");
        builder.setBolt("nameentity", new NameEntitiyBolt(), 4).shuffleGrouping("sentiment");
        builder.setBolt("hashtag", new HastagBolt(), 1).shuffleGrouping("sentiment");
        builder.setBolt("nameentityLossycount", new LossycountBolt(), 4).shuffleGrouping("nameentity");
        builder.setBolt("hashtagLossycount", new LossycountBolt(), 4).shuffleGrouping("hashtag");

        //builder.setBolt("name_entity_count", new RollingCountingLossyAlg(), 4).fieldsGrouping("name_entity", new Fields("entity"));
        //builder.setBolt("hashtag_count", new RollingCountingLossyAlg(), 4).shuffleGrouping("hashtag");//.fieldsGrouping("hashtag", new Fields("entity"));

//        builder.setBolt("name_entity-intermediate-ranking", new IntermediateRankingBolt(100), 4).fieldsGrouping("name_entity_count", new Fields(
//                "entity"));
//        builder.setBolt("name_entity-total-ranking", new TopRankingBolt(100)).globalGrouping("name_entity-intermediate-ranking");
//        builder.setBolt("hashtag-intermediate-ranking", new IntermediateRankingBolt(100), 4).fieldsGrouping("hashtag_count", new Fields(
//                "entity"));
//        builder.setBolt("hashtag-total-ranking", new TopRankingBolt(100)).globalGrouping("hashtag-intermediate-ranking");
//        builder.setBolt("logger1", new LoggerBolt("~/tmp/log1.txt")).shuffleGrouping("name_entity-total-ranking");
        builder.setBolt("logger1", new LoggerBolt(logFile1)).shuffleGrouping("nameentityLossycount");
        builder.setBolt("logger2", new LoggerBolt(logFile2)).shuffleGrouping("hashtagLossycount");


        //builder.setBolt("hashtag-ranking-print", new FileWriterBolt("HASHTAG_RANKING.txt")).shuffleGrouping("hashtag-total-ranking");



        Config conf = new Config();
        conf.setDebug(false);
//
//        if (runOnCluster!=null) {
//            conf.setNumWorkers(4);
//
//            StormSubmitter.submitTopology("storm-sentiments-twitter", conf, builder.createTopology());
//        }
//        else {
//            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("storm-sentiments-twitter", conf, builder.createTopology());

           // Utils.sleep(10000); //for testing purpose

            //cluster.shutdown();
    //    }


//        conf.setMaxTaskParallelism(3);
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("storm-sentiments-twitter", conf, builder.createTopology());
        //Utils.sleep(100000);
        //cluster.shutdown();
    }


}
