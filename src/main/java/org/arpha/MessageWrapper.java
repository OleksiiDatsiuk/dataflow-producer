package org.arpha;

public class MessageWrapper {

    private final String type = "PRODUCER";
    private String topic;
    private String content;

    public MessageWrapper(String topic, String content) {
        this.topic = topic;
        this.content = content;
    }

    public MessageWrapper() {
    }

    public String getType() {
        return "PRODUCER";
    }

    public String getTopic() {
        return this.topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getContent() {
        return this.content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
