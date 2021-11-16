package com.james.kafka.deserializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    private Class<T> tClass;

    public JsonDeserializer() {}

    public JsonDeserializer(Class<T> tClass) {
        this.tClass = tClass;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        T data;
        try {
            data = objectMapper.readValue(bytes, tClass);
        }
        catch (Exception ex) {
            throw new SerializationException(ex);
        }

        return data;
    }

    @Override
    public void close() {

    }
}
