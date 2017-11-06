import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;
//import twitter4j.TwitterStream;


public class TwitterStreamer {


    public static void main(String[] args) throws TwitterException, IOException {



        String consumerKey = "wXdz6TjwSw13yD2W0Edu8KBcc";
        String consumerSecret = "fh9kF4RDjv2jcV9EogGRiXCqsyaJqW9maOTXWTEv7IHkej1QlV";
        String accessToken = "926422936826863616-ljuZgL3wtXOBipuY2KDPQ8ezULbInTJ";
        String accessTokenSecret = "GgPHDzba3R7cYZXpbTCkdB2aeVT8BY0JUnY3tnEw0OwbP";

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setOAuthConsumerKey(consumerKey)
                .setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(accessToken)
                .setOAuthAccessTokenSecret(accessTokenSecret);






        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());

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

        TwitterStream twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();





        //twitterStream.shutdown();
        twitterStream.addListener(listener);
        twitterStream.sample();



















    }
}
