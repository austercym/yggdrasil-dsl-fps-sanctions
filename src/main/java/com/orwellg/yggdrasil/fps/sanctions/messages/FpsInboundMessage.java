package com.orwellg.yggdrasil.fps.sanctions.messages;


import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

import com.google.gson.GsonBuilder;
import com.orwellg.umbrella.avro.types.payment.iso20022.pacs.pacs008_001_05.CreditTransferTransaction19;
import com.orwellg.umbrella.avro.types.payment.iso20022.pacs.pacs008_001_05.Document;
import com.orwellg.umbrella.avro.types.payment.iso20022.pacs.pacs008_001_05.GroupHeader49;

public class FpsInboundMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Integer MAX_ORIGIN_CUSTOMER_NAME = 140;

    private String PaymentId;
    private String PaymentType;
    private String FPID;

    private Document PaymentDocument;


    public Document getPaymentDocument() {
        return PaymentDocument;
    }

    public void setPaymentDocument(Document PaymentDocument) {
        this.PaymentDocument = PaymentDocument;
    }

    public CreditTransferTransaction19 getTransaction19() {
        return getPaymentDocument().getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0);
    }

    public GroupHeader49 getGroupHeader49() {
        return getPaymentDocument().getFIToFICstmrCdtTrf().getGrpHdr();
    }

    public String getCurrency() {
        if (getTransaction19()!=null && getTransaction19().getIntrBkSttlmAmt()!=null && getTransaction19().getIntrBkSttlmAmt().getCcy()!=null) {
            return  getTransaction19().getIntrBkSttlmAmt().getCcy().trim();
        } else {
            return  null;
        }
    }

    public LocalDate getDateSent() {
        if (StringUtils.isNotBlank(getTransaction19().getIntrBkSttlmDt())){
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.getPattern());
            return LocalDate.parse(getTransaction19().getIntrBkSttlmDt(), formatter);
        } else {
            return null;
        }
    }

    public String getCreditorIBAN() {
        if (getTransaction19().getCdtrAcct() != null && getTransaction19().getCdtrAcct().getId() != null) {
            return getTransaction19().getCdtrAcct().getId().getIBAN();
        } else {
            return null;
        }
    }

    public String getCreditorSortAccount() {
        if (getTransaction19().getCdtrAcct() != null && getTransaction19().getCdtrAcct().getId() != null && getTransaction19().getCdtrAcct().getId().getOthr() != null) {
            return getTransaction19().getCdtrAcct().getId().getOthr().getId();
        } else {
            return null;
        }
    }

    public String getCreditorSortCode() {
        if (getTransaction19().getCdtrAgt() != null && getTransaction19().getCdtrAgt().getFinInstnId() != null && getTransaction19().getCdtrAgt().getFinInstnId().getClrSysMmbId() != null) {
            return getTransaction19().getCdtrAgt().getFinInstnId().getClrSysMmbId().getMmbId();
        } else {
            return null;
        }
    }

    public String getCreditorAccountNumber() {
        if (getTransaction19().getCdtrAgtAcct() != null && getTransaction19().getCdtrAgtAcct().getTp() != null) {
            return getTransaction19().getCdtrAgtAcct().getTp().getPrtry();
        } else {
            return null;
        }
    }

    public String getDbtrBIC() {
        if (getTransaction19().getDbtrAgt() != null && getTransaction19().getDbtrAgt().getFinInstnId()!= null) {
            return getTransaction19().getDbtrAgt().getFinInstnId().getBICFI();
        } else {
            return null;
        }
    }

    public BigDecimal getAmount() {
        if (getTransaction19()!=null && getTransaction19().getIntrBkSttlmAmt()!= null && getTransaction19().getIntrBkSttlmAmt().getValue()!=null) {
            return getTransaction19().getIntrBkSttlmAmt().getValue().getValue();
        } else {
            return null;
        }
    }

    public String getOriginCustomerAccountName() {
        if (getTransaction19().getDbtrAcct() != null && getTransaction19().getDbtrAcct().getNm() != null) {
            return getTransaction19().getDbtrAcct().getNm();
        } else {
            return null;
        }
    }

    public Boolean validOriginCustomerAccountName(){
        if (getOriginCustomerAccountName()!=null ){
            if (getOriginCustomerAccountName().length()>MAX_ORIGIN_CUSTOMER_NAME){
                return Boolean.FALSE;
            } else {
                return Boolean.TRUE;
            }
        } else {
            return Boolean.TRUE;
        }
    }

    public List<String> getOriginCustomerAddress() {
        if (getTransaction19().getDbtrAcct() != null && getTransaction19().getDbtrAcct().getNm() != null) {
            return getTransaction19().getDbtr().getPstlAdr().getAdrLine();
        } else {
            return null;
        }
    }

    public Boolean isEmptyOriginCustomerAddress(){
        if (getOriginCustomerAddress()==null){
            return true;
        } else {
            return getOriginCustomerAddress().isEmpty();
        }
    }

    public String getBeneficiaryCustomerAccountName() {
        if (getTransaction19().getCdtr() != null && getTransaction19().getCdtr().getNm() != null) {
            return getTransaction19().getCdtr().getNm();
        } else {
            return null;
        }
    }

    public List<String> getBeneficiaryCustomerAddress() {
        if (getTransaction19().getCdtrAcct() != null && getTransaction19().getCdtrAcct().getNm() != null) {
            return getTransaction19().getCdtr().getPstlAdr().getAdrLine();
        } else {
            return null;
        }
    }

    public Boolean isEmptyBeneficiaryCustomerAddress(){
        if (getBeneficiaryCustomerAddress()==null){
            return true;
        } else {
            return getBeneficiaryCustomerAddress().isEmpty();
        }
    }

    public String getPaymentId() {
        return PaymentId;
    }

    public void setPaymentId(String paymentId) {
        PaymentId = paymentId;
    }

    public String getPaymentType() {
        return PaymentType;
    }

    public void setPaymentType(String paymentType) {
        PaymentType = paymentType;
    }

    public String getFPID() {
        return FPID;
    }

    public void setFPID(String FPID) {
        this.FPID = FPID;
    }

    public Boolean isPOO(){
        if (getDbtrBIC()!=null){
            return Boolean.TRUE;
        } {
            return Boolean.FALSE;
        }
    }

    public String getPaymentTypeCode() {
        String lclInstrm = getInboundPaymentTypeParameter();
        if (lclInstrm != null) {
            int ind = lclInstrm.indexOf("/");
            return (ind > 0 ? lclInstrm.substring(ind+1, ind+3) : lclInstrm);
        } else {
            return null;
        }
    }

    public String getInboundPaymentTypeParameter() {
        if (getTransaction19()!=null && getTransaction19().getPmtTpInf()!=null && getTransaction19().getPmtTpInf().getLclInstrm()!=null){
            return getTransaction19().getPmtTpInf().getLclInstrm().getPrtry();
        } else {
            return null;
        }
    }

    public BigDecimal getOriginalAmount() {
        if (getTransaction19()!=null && getTransaction19().getInstdAmt()!= null && getTransaction19().getInstdAmt().getValue()!=null) {
            return getTransaction19().getInstdAmt().getValue().getValue();
        } else {
            return null;
        }
    }

    public BigDecimal getExchangeRate() {
        if (getTransaction19()!=null && getTransaction19().getXchgRate()!= null && getTransaction19().getXchgRate().getValue()!=null) {
            return getTransaction19().getXchgRate().getValue();
        } else {
            return null;
        }
    }

    public String getTRN() {
        if (getTransaction19()!=null && getTransaction19().getPmtId() != null ) {
            return getTransaction19().getPmtId().getTxId();
        } else {
            return null;
        }
    }

    public String getOriginCreditInstitution() {
        if (getTransaction19()!=null && getTransaction19().getDbtrAgt() !=null && getTransaction19().getDbtrAgt().getFinInstnId() !=null && getTransaction19().getDbtrAgt().getFinInstnId().getClrSysMmbId()!= null ) {
            return getTransaction19().getDbtrAgt().getFinInstnId().getClrSysMmbId().getMmbId();
        } else {
            return null;
        }
    }

    public String getOriginCustomerNumber() {
        if (getTransaction19().getDbtrAcct() != null && getTransaction19().getDbtrAcct().getId() != null && getTransaction19().getDbtrAcct().getId().getOthr() != null) {
            return getTransaction19().getDbtrAcct().getId().getOthr().getId();
        } else {
            return null;
        }
    }

    public String getOriginalFPID() {
        if (getTransaction19().getRmtInf() != null && getTransaction19().getRmtInf().getStrd() != null &&
                getTransaction19().getRmtInf().getStrd().get(0) != null && getTransaction19().getRmtInf().getStrd().get(0).getAddtlRmtInf() != null) {
            for (String original : getTransaction19().getRmtInf().getStrd().get(0).getAddtlRmtInf()){
                if (original.contains("/ORGNFPID/")){
                    return StringUtils.substringAfter(original,"/ORGNFPID/");
                }
            }
        }
        return null;
    }

    public String getOriginalPaymentId() {
        String origFPID =  getOriginalFPID();
        if (origFPID!=null){
            return StringUtils.substringBefore(origFPID," ");
        }
        return null;
    }

    @Override
    public String toString() {
        return new GsonBuilder().disableHtmlEscaping().create().toJson(this.getPaymentDocument()).toString();
    }



}
