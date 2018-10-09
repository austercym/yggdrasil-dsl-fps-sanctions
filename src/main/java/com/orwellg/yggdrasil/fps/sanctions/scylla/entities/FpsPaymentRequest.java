package com.orwellg.yggdrasil.fps.sanctions.scylla.entities;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import com.orwellg.umbrella.avro.types.payment.iso20022.pacs.pacs008_001_05.Document;

public class FpsPaymentRequest {

    private String FPID;
    private String paymentId;
    private LocalDateTime requestTimeStamp;
    private BigDecimal transactionAmount;
    
    private String dbtrAccountId;
    private String cdtrAccountId;
    private Document document;
    
    private String direction;
    
    private String currency;
    private String paymentType;

    public FpsPaymentRequest(String FPID, String paymentId, Date requestTimeStamp, BigDecimal transactionAmount, String dbtrAccountId, String cdtrAccountId, String currency, Document document, String direction, String paymentType){
        this.FPID = FPID;
        this.paymentId = paymentId;
        this.requestTimeStamp = LocalDateTime.ofInstant(requestTimeStamp.toInstant(), ZoneId.systemDefault());
        this.transactionAmount = transactionAmount;
        this.dbtrAccountId = dbtrAccountId;
        this.cdtrAccountId = cdtrAccountId;
        this.currency = currency;
        this.document = document;
        this.direction = direction;
        this.paymentType = paymentType;
    }

    public String getFPID() {
        return FPID;
    }

    public void setFPID(String FPID) {
        this.FPID = FPID;
    }

    public String getPaymentId() {
        return paymentId;
    }

    public void setPaymentId(String paymentId) {
        this.paymentId = paymentId;
    }

    public LocalDateTime getRequestTimeStamp() {
        return requestTimeStamp;
    }

    public void setRequestTimeStamp(LocalDateTime requestTimeStamp) {
        this.requestTimeStamp = requestTimeStamp;
    }

    public BigDecimal getTransactionAmount() {
        return transactionAmount;
    }

    public void setTransactionAmount(BigDecimal transactionAmount) {
        this.transactionAmount = transactionAmount;
    }

	public String getDbtrAccountId() {
		return dbtrAccountId;
	}

	public void setDbtrAccountId(String dbtrAccountId) {
		this.dbtrAccountId = dbtrAccountId;
	}

	public String getCdtrAccountId() {
		return cdtrAccountId;
	}

	public void setCdtrAccountId(String cdtrAccountId) {
		this.cdtrAccountId = cdtrAccountId;
	}

	public String getCurrency() {
		return currency;
	}

	public void setCurrency(String currency) {
		this.currency = currency;
	}

	public Document getDocument() {
		return document;
	}

	public void setDocument(Document document) {
		this.document = document;
	}

	public String getDirection() {
		return direction;
	}

	public void setDirection(String direction) {
		this.direction = direction;
	}

	public String getPaymentType() {
		return paymentType;
	}

	public void setPaymentType(String paymentType) {
		this.paymentType = paymentType;
	}
	
}
