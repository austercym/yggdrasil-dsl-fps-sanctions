package com.orwellg.yggdrasil.fps.sanctions.messages;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FpsErrorMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String FPS_ERROR_VALIDATION = "FPS_ERROR_VALIDATION";
    public static final String FPS_NOT_EXISTING_ACCOUNT = "FPS_NOT_EXISTING_ACCOUNT";
    public static final String FPS_NOT_VALID_ACCOUNT = "FPS_NOT_VALID_ACCOUNT";

    private List<String> messages = new ArrayList<>();
    private Integer code;

    public FpsErrorMessage(List<String> messages, Integer code){
        this.code = code;
        this.messages = messages;
    }

    public FpsErrorMessage(Integer code, String message){
        this.code = code;
        this.getMessages().add(message);
    }

    public FpsErrorMessage(){}

    public FpsErrorMessage(Integer code){
        this.code = code;
    }

    public List<String> getMessages() {
        return messages;
    }

    public void setMessages(List<String> messages) {
        this.messages = messages;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }
}
