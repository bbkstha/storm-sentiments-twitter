package resources;

//import org.apache.log4j.Logger;
import org.apache.storm.tuple.Tuple;

public final class TopRankingBolt extends AbstractRankerBolt {

    private static final long serialVersionUID = -8447525895532302198L;
    //private static final Logger LOG = Logger.getLogger(TopRankingBolt.class);

    public TopRankingBolt() {
        super();
    }

    public TopRankingBolt(int topN) {
        super(topN);
    }

    public TopRankingBolt(int topN, int emitFrequencyInSeconds) {
        super(topN, emitFrequencyInSeconds);
    }

    @Override
    void updateRankingsWithTuple(Tuple tuple) {
        Rankings rankingsToBeMerged = (Rankings) tuple.getValue(0);
        super.getRankings().updateWith(rankingsToBeMerged);
        super.getRankings().pruneZeroCounts();
    }

//    @Override
//    Logger getLogger() {
//        return LOG;
//    }

}
