package resources;

//import org.apache.log4j.Logger;
import org.apache.storm.tuple.Tuple;

public final class IntermediateRankingBolt extends AbstractRankerBolt {

    private static final long serialVersionUID = -1369800530256637409L;
    //private static final Logger LOG = Logger.getLogger(IntermediateRankingBolt.class);

    public IntermediateRankingBolt() {
        super();
    }

    public IntermediateRankingBolt(int topN) {
        super(topN);
    }

    public IntermediateRankingBolt(int topN, int emitFrequencyInSeconds) {
        super(topN, emitFrequencyInSeconds);
    }

    @Override
    void updateRankingsWithTuple(Tuple tuple) {
        Rankable rankable = RankableObjectWithFields.from(tuple);
        super.getRankings().updateWith(rankable);
    }
//
//    @Override
//    Logger getLogger() {
//        return LOG;
//    }
}
