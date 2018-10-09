package com.orwellg.yggdrasil.fps.sanctions.topology.bolts;


import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.payment.fps.FPSSanctionsAction;
import com.orwellg.umbrella.commons.config.params.ScyllaParams;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.utils.scylla.ScyllaManager;
import com.orwellg.yggdrasil.commons.utils.constants.Constants;
import com.orwellg.yggdrasil.fps.sanctions.config.FpsSanctionsTopologyConfigFactory;
import com.orwellg.yggdrasil.fps.sanctions.exception.FPSPaymentRequestNotFoundException;
import com.orwellg.yggdrasil.fps.sanctions.scylla.FpsPaymentRequestNoSqlDao;
import com.orwellg.yggdrasil.fps.sanctions.scylla.entities.FpsPaymentRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class FpsOriginalPaymentBolt extends BasicRichBolt {

    public final static Logger LOG = LogManager.getLogger(FpsOriginalPaymentBolt.class);

    private static final long serialVersionUID = 1L;

    private static final String FPS_ERROR_RETURN_STREAM = "fpsErrorReturnStream";
    
    private String zookeeperHost;
    private FpsPaymentRequestNoSqlDao fpsPaymentRequestNoSqlDao;


    @SuppressWarnings("rawtypes")
	@Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        zookeeperHost = (String) stormConf.get(Constants.ZK_HOST_LIST_PROPERTY);
        ScyllaParams topologyScyllaParams = FpsSanctionsTopologyConfigFactory.getDSLTopologyConfig(zookeeperHost).getScyllaConfig().getScyllaParams();
        ScyllaManager man = ScyllaManager.getInstance(topologyScyllaParams);
        fpsPaymentRequestNoSqlDao = new FpsPaymentRequestNoSqlDao(man,topologyScyllaParams.getKeyspace());
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(new String[] {"key", "processId", "eventData", "fpsPaymentRequest"}));
        addFielsDefinition(FPS_ERROR_RETURN_STREAM, Arrays.asList(new String[] {"key", "processId", "eventData", "errors"}));
    }

    @Override
    public void execute(Tuple tuple) {
	 	LOG.info("Getting ready for executing OriginalPaymentBolt Bolt. Tuple values: {}", tuple);
        Map<String, Object> values = new HashMap<>();

        String eventKey = tuple.getStringByField("key");
        String pmtId = tuple.getStringByField("processId");
        FPSSanctionsAction fpsSanctionsAction = (FPSSanctionsAction) tuple.getValueByField("eventData");
        
        try {
            LOG.info("[PmtId: {}] Received FPS inbound payment to validate customer account with event key {}", pmtId, eventKey);
            String paymentId;
            paymentId = fpsSanctionsAction.getPaymentId();

            FpsPaymentRequest fpsPaymentRequest = findPaymentRequest(paymentId);

            if (fpsPaymentRequest==null){
                throw new FPSPaymentRequestNotFoundException(fpsSanctionsAction.getPaymentId());
            }
            LOG.info("[PmtId: {}] Found customer account internal id: {}", pmtId, new Gson().toJson(fpsPaymentRequest));

            values.put("key", pmtId);
            values.put("processId", pmtId);
            values.put("eventData", fpsSanctionsAction);
            values.put("fpsPaymentRequest", fpsPaymentRequest);

            LOG.info("Sending tuple values. Tuple: {}", new Gson().toJson(values));
            send(tuple, values);
        } catch(Exception ex) {
            LOG.error("[PmtId: {}] Error validating customer account for inbound payment. Error message: {}", pmtId, ex.getMessage(), ex);
            send(FPS_ERROR_RETURN_STREAM, tuple, values);
        }
    }



    private FpsPaymentRequest findPaymentRequest(String paymentId){
        FpsPaymentRequest paymentRequest = fpsPaymentRequestNoSqlDao.findFPSPaymentRequestByPaymentId(paymentId);
        return paymentRequest;
    }
}
