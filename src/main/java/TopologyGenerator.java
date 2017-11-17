
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;


public class TopologyGenerator {




    public static void main(String[] args) throws Exception {

        String logFile1 = "/s/chopin/b/grad/bbkstha/pa2log/Top100NameEntity.txt";
        String logFile2 = "/s/chopin/b/grad/bbkstha/pa2log/Top100Hashtag.txt";
        Boolean runOnCluster=false;
        if (args.length >=1) {
            runOnCluster = true;
        }

        String tempFile1 = "/s/chopin/b/grad/bbkstha/pa2log/log1.txt";
        String tempFile2 = "/s/chopin/b/grad/bbkstha/pa2log/log2.txt";
        String tempFile3 = "/s/chopin/b/grad/bbkstha/pa2log/log3.txt";
        String tempFile4 = "/s/chopin/b/grad/bbkstha/pa2log/log4.txt";
        String tempFile5 = "/s/chopin/b/grad/bbkstha/pa2log/log5.txt";




        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter", new TwitterStreammingSpout(),1);
        builder.setBolt("language", new LanguageDetectorBolt(), 8).shuffleGrouping("twitter");
        builder.setBolt("sentiment", new SentimentBolt(tempFile1), 8).shuffleGrouping("language");
        builder.setBolt("nameentity", new NameEntitiyBolt(tempFile2), 8).shuffleGrouping("sentiment");
        builder.setBolt("hashtag", new HastagBolt(tempFile3), 8).shuffleGrouping("sentiment");
        builder.setBolt("nameentityLossycount", new LossycountBolt(tempFile4), 8).fieldsGrouping("nameentity", new Fields("entity"));
        builder.setBolt("hashtagLossycount", new LossycountBolt(tempFile5), 8).fieldsGrouping("hashtag", new Fields("entity"));
        builder.setBolt("logger1", new LoggerBolt(logFile1), 1).globalGrouping("nameentityLossycount");
        builder.setBolt("logger2", new LoggerBolt(logFile2), 1).globalGrouping("hashtagLossycount");

        Config conf = new Config();
        conf.setDebug(false);
        //conf.setMaxSpoutPending(5000);

        if (runOnCluster) {
            conf.setNumWorkers(16);
            StormSubmitter.submitTopology("storm-sentiments-twitter", conf, builder.createTopology());
        }
        else {
            //conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("storm-sentiments-twitter", conf, builder.createTopology());
        }

    }


}
