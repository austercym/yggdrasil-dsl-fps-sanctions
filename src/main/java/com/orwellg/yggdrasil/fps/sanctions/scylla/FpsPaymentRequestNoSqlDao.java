package com.orwellg.yggdrasil.fps.sanctions.scylla;

import com.datastax.driver.core.*;
import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.payment.iso20022.pacs.pacs008_001_05.Document;
import com.orwellg.umbrella.commons.types.fps.PaymentStatus;
import com.orwellg.umbrella.commons.utils.scylla.ScyllaManager;
import com.orwellg.yggdrasil.fps.sanctions.scylla.entities.FpsPaymentRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.List;


public class FpsPaymentRequestNoSqlDao {

	protected ScyllaManager man;
	protected Session ses;


	public FpsPaymentRequestNoSqlDao() {
	}

	public FpsPaymentRequestNoSqlDao(ScyllaManager man,String keySpace) {
		this.man = man;
		this.ses = man.getSession(keySpace);
	}

	public FpsPaymentRequest findFPSPaymentRequestByPaymentId(String paymentId) {

		String selectQuery = "select * from ipagoo.FPSPaymentRequest where PaymentId = ? LIMIT 1";
		PreparedStatement st = ses.prepare(selectQuery);
		BoundStatement select = st.bind(paymentId);
		ResultSet rs = ses.execute(select);
		Row result = rs.one();
        FpsPaymentRequest response = null;
		if (result!=null){
			Document document = null;
			try {
				Gson gson = new Gson();
				String paymentMessage = result.getString("PaymentMessage");
				if (!paymentMessage.trim().isEmpty()) {
					document = gson.fromJson(paymentMessage, Document.class);
				} 
			} catch (Exception e) {
				document = null;
			}
		    response = new FpsPaymentRequest(result.getString("FPID"),result.getString("PaymentId"),result.getTimestamp("CreDtTm"),result.getDecimal("IntrBkSttlmAmt"),result.getString("DbtrAccountId"), result.getString("CdtrAccountId"), result.getString("IntrBkSttlmCcy"), document, result.getString("direction"), result.getString("paymenttype"));
		}
		return response;
	}



}
