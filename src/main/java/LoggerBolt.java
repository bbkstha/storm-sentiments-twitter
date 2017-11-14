import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class LoggerBolt extends BaseRichBolt {


    PrintWriter writer;
    int count = 0;
    private OutputCollector collector;
    private String fileName;

    protected LinkedBlockingQueue<Tuple> queue = new LinkedBlockingQueue<Tuple>();
    int batchSize = 10;
    int batchIntervalInSec = 10;
    long lastBatchProcessTimeSeconds = 0;


    public LoggerBolt(String filename){
        this.fileName = filename;
    }


    @Override
    public Map<String, Object> getComponentConfiguration(){
        Config conf = new Config();
        int tickFrequencyInSeconds = 10;
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
        return conf;
    }

    private static boolean isTickTuple(Tuple tuple){
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    //@Override
    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {
        collector = outputCollector;
        try {
            writer = new PrintWriter(fileName, "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

//    //@Override
//    public void execute(Tuple tuple) {
//        String timestamp = tuple.getStringByField("timestamp");
//        String entity = tuple.getStringByField("entitiy");
//        Integer sentiment = tuple.getIntegerByField("sentiment");
//        writer.print(timestamp);
//        writer.print(entity);
//        writer.print(sentiment);
//        writer.flush();
//        // Confirm that this tuple has been treated.
//        collector.ack(tuple);
//    }
        public void execute(Tuple tuple) {
            if (isTickTuple(tuple)) {

                if ((System.currentTimeMillis() / 1000 - lastBatchProcessTimeSeconds) >= batchIntervalInSec) {

//                    LOG.debug("Current queue size is " + this.queue.size()
//
//                            + ". But received tick tuple so executing the batch");

                    finishBatch();

                } else {

//                    LOG.debug("Current queue size is " + this.queue.size()
//
//                            + ". Received tick tuple but last batch was executed "
//
//                            + (System.currentTimeMillis() / 1000 - lastBatchProcessTimeSeconds)
//
//                            + " seconds back that is less than " + batchIntervalInSec
//
//                            + " so ignoring the tick tuple");

                }

            } else {

                // Add the tuple to queue. But don't ack it yet.

                this.queue.add(tuple);

                int queueSize = this.queue.size();

                //LOG.debug("current queue size is " + queueSize);

//                if (queueSize >= batchSize) {
//
//                    LOG.debug("Current queue size is >= " + batchSize
//
//                            + " executing the batch");
//
//                    finishBatch();
//
//                }

            }

        }

    public void finishBatch() {

        //LOG.debug("Finishing batch of size " + queue.size());

        lastBatchProcessTimeSeconds = System.currentTimeMillis() / 1000;

        List<Tuple> tuples = new ArrayList<Tuple>();

        queue.drainTo(tuples);

        ///rank tuples based on lossycount////

        //Collections.sort(tuples, (Tuple t1, Tuple t2) -> t1.getIntegerByField("lossycount").compareTo(t2.getIntegerByField("lossycount")));
        //Collections.reverse(tuples);

        //BulkRequestBuilder bulkRequest = client.prepareBulk();

        //BulkResponse bulkResponse = null;
        writer.println("<" + lastBatchProcessTimeSeconds + ">");


        int tupleSize = tuples.size();
        if (tupleSize > 100)
            tupleSize = 100;
        for (int i = 0; i < tupleSize; i++) {
            // Prepare your batch here (may it be JDBC, HBase, ElasticSearch, Solr or

            // anything else.

            String entity = tuples.get(i).getStringByField("entity");
            Integer sentiment = tuples.get(i).getIntegerByField("sentiment");
            Integer fre = tuples.get(i).getIntegerByField("lossycount");
            //Integer lossyCount = tuples.get(i).getIntegerByField("lossycount");
            writer.print("<" + entity + ":" + sentiment + "> and count:" + fre +"\n");
            writer.flush();
            //break;
            // Confirm that this tuple has been treated.
            //collector.ack(tuple);
        }
    }
//        try {
//
//            // Execute bulk request and get individual tuple responses back.
//
//            bulkResponse = bulkRequest.execute().actionGet();
//
//            BulkItemResponse[] responses = bulkResponse.getItems();
//
//            BulkItemResponse response = null;
//
//            LOG.debug("Executed the batch. Processing responses.");
//
//            for (int counter = 0; counter < responses.length; counter++) {
//
//                response = responses[counter];
//
//                if (response.isFailed()) {
//
//                    ElasticSearchDocument failedEsDocument = this.tupleMapper
//
//                            .mapToDocument(tuples.get(counter));
//
//                    LOG.error("Failed to process tuple # " + counter);
//
//                    this.collector.fail(tuples.get(counter));
//
//                } else {
//
//                    LOG.debug("Successfully processed tuple # " + counter);
//
//                    this.collector.ack(tuples.get(counter));
//
//                }
//
//            }
//
//        } catch (Exception e) {
//
//            LOG.error("Unable to process " + tuples.size() + " tuples", e);
//
//            // Fail entire batch
//
//            for (Tuple tuple : tuples) {
//
//                this.collector.fail(tuple);
//
//            }
//
//        }

   // }

    //@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    //@Override
    public void cleanup() {
        writer.close();
        super.cleanup();

    }
}
