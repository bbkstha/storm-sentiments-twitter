import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterStreammingSpout extends BaseRichSpout {

    private final String accessTokenSecret;
    private final String accessToken;
    private final String consumerSecret;
    private final String consumerKey;
    SpoutOutputCollector collector;
    private LinkedBlockingQueue statusHolder;
    TwitterStream twitterStream;
    PrintWriter writer;
    int count = 0;



    public TwitterStreammingSpout() {

        consumerKey = "wXdz6TjwSw13yD2W0Edu8KBcc";
        consumerSecret = "fh9kF4RDjv2jcV9EogGRiXCqsyaJqW9maOTXWTEv7IHkej1QlV";
        accessToken = "926422936826863616-ljuZgL3wtXOBipuY2KDPQ8ezULbInTJ";
        accessTokenSecret = "GgPHDzba3R7cYZXpbTCkdB2aeVT8BY0JUnY3tnEw0OwbP";

    }






    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {




            statusHolder = new LinkedBlockingQueue();
            collector = spoutOutputCollector;
            ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
            configurationBuilder.setOAuthConsumerKey(consumerKey)
                    .setOAuthConsumerSecret(consumerSecret)
                    .setOAuthAccessToken(accessToken)
                    .setOAuthAccessTokenSecret(accessTokenSecret);
            twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();

        try {
            writer = new PrintWriter("/home/bbkstha/Desktop/pa2log/log1.txt", "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

            StatusListener listener = new StatusListener() {
                @Override
                public void onStatus(Status status) {
                    writer.println((count++)+":"+status.getText());
                    writer.flush();
                    //System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
                    statusHolder.offer(status.getText());

                }

                @Override
                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                    System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
                }

                @Override
                public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                    System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
                }

                @Override
                public void onScrubGeo(long userId, long upToStatusId) {
                    System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
                }

                @Override
                public void onStallWarning(StallWarning warning) {
                    System.out.println("Got stall warning:" + warning);
                }

                @Override
                public void onException(Exception ex) {
                    ex.printStackTrace();
                }
            };

            twitterStream.addListener(listener);
            twitterStream.sample();

        }

    @Override
    public void nextTuple() {
        // emit tweets
        Object s = statusHolder.poll();
        if (s == null) {
            Utils.sleep(1000);
        } else {
            collector.emit(new Values(s));

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("tweet"));
    }

    @Override
    public void close() {
        writer.close();
        twitterStream.shutdown();
        super.close();
    }

}
