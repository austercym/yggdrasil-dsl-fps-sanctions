package com.orwellg.yggdrasil.fps.sanctions.topology.bolts;


import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.event.EntityIdentifierType;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.event.EventType;
import com.orwellg.umbrella.avro.types.event.ProcessIdentifierType;
import com.orwellg.umbrella.avro.types.payment.fps.FPSOutboundPaymentResponse;
import com.orwellg.umbrella.avro.types.payment.fps.FPSSanctionsAction;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.utils.avro.RawMessageUtils;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.commons.utils.enums.FPSEvents;
import com.orwellg.yggdrasil.fps.sanctions.scylla.entities.FpsPaymentRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.text.SimpleDateFormat;
import java.util.*;


public class FpsCreateReturnResponseBolt extends BasicRichBolt {

	private static final long serialVersionUID = 1L;

	public final static Logger LOG = LogManager.getLogger(FpsCreateReturnResponseBolt.class);

    private static final String FPS_SANCTIONS_RETURN_RESPONSE = "com.orwellg.yggdrasil.dsl.fps.sanctions.pass.accounting.response.1";

    private static final String FPS_KAFKA_RETURN_RESPONSE_STREAM = "fpsKafkaReturnResponseStream";

    private static final String RETURN_TXSTS = "ACSP";
    private static final String RETURN_CODE = "0081";

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(new String[] {"key", "processId", "eventData"}));
        addFielsDefinition(FPS_KAFKA_RETURN_RESPONSE_STREAM,Arrays.asList(new String[] {"key", "topic", "message"}));
    }

    @Override
    public void execute(Tuple input) {
        String eventKey = (String) input.getValueByField("key");
        String pmtId = (String) input.getValueByField("processId");
        FpsPaymentRequest fpsPaymentRequest = (FpsPaymentRequest) input.getValueByField("eventData");

        LOG.info("[PmtId: {}] Return response inbound payment ", pmtId);

        sendResponseToKafka(input,pmtId,eventKey,fpsPaymentRequest);

        Map<String, Object> values = new HashMap<>();
        values.put("key", pmtId);
        values.put("processId", pmtId);
        values.put("eventData", fpsPaymentRequest);

        send(input, values);
    }

    private void sendResponseToKafka(Tuple input,String pmtId,String eventKey,FpsPaymentRequest fpsSanctionsAction){
        FPSOutboundPaymentResponse message=null;
        
        Map<String, Object> values = new HashMap<>();

        values.put("key", pmtId);
        values.put("topic", FPS_SANCTIONS_RETURN_RESPONSE);
        String base64Event;
        try {
            String serializedData = new Gson().toJson(fpsSanctionsAction);
            Event event = generateEvent(eventKey, pmtId, FPSEvents.FPS_SANCTION_RECEIVED.getEventName(), serializedData);
            base64Event = RawMessageUtils.encodeToString(Event.SCHEMA$, event);
        } catch (Exception e){
            LOG.error(e.getMessage());
            base64Event = "base64 failed at serialization.... ";
        }

        LOG.info("[PmtId: {}] Sending return payment inbound to topic: {} ", pmtId, FPS_SANCTIONS_RETURN_RESPONSE);
        values.put("message",base64Event);

        send(FPS_KAFKA_RETURN_RESPONSE_STREAM,input, values);
    }

    private Event generateEvent(String parentKey, String processId, String eventName, String serializedData) {

        // Create the event type
        EventType eventType = new EventType();
        String eventKey = UUID.randomUUID().toString();
        eventType.setKey("EVENT-"+eventKey);
        eventType.setParentKey(parentKey);
        eventType.setName(eventName);
        eventType.setVersion(Constants.getDefaultEventVersion());
        eventType.setSource(this.getClass().getSimpleName());
        SimpleDateFormat format = new SimpleDateFormat(Constants.getDefaultEventTimestampFormat());
        eventType.setTimestamp(format.format(new Date()));

        eventType.setData(serializedData);

        ProcessIdentifierType processIdentifier = new ProcessIdentifierType();
        processIdentifier.setUuid(processId);

        EntityIdentifierType entityIdentifier = new EntityIdentifierType();
        entityIdentifier.setEntity(Constants.IPAGOO_ENTITY);
        entityIdentifier.setBrand(Constants.IPAGOO_BRAND);

        // Create the corresponden event
        Event event = new Event();
        event.setEvent(eventType);
        event.setProcessIdentifier(processIdentifier);
        event.setEntityIdentifier(entityIdentifier);


        return event;
    }
}
