package com.cj.flink.streaming.redis;

import net.sf.json.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class ObjectDeserializationSchema implements DeserializationSchema<JSONObject> {
    @Override
    public JSONObject deserialize(byte[] bytes) throws IOException {
        return null;
    }

    @Override
    public boolean isEndOfStream(JSONObject jsonObject) {
        return false;
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return null;
    }
}
