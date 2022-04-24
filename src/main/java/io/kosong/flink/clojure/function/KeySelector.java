package io.kosong.flink.clojure.function;

import clojure.lang.APersistentMap;
import clojure.lang.IFn;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.util.Map;

import static io.kosong.flink.clojure.function.Utils.keyword;
import static io.kosong.flink.clojure.function.Utils.resolve;

public class KeySelector implements org.apache.flink.api.java.functions.KeySelector, ResultTypeQueryable {

    private final TypeInformation returnType;
    private final IFn getKeyFn;

    public KeySelector(APersistentMap args) {
        getKeyFn = (IFn) resolve(args.invoke(keyword("getKey")));
        returnType = (TypeInformation) args.invoke(keyword("returns"));
    }

    @Override
    public Object getKey(Object value) throws Exception {
        return getKeyFn.invoke(value);
    }

    @Override
    public TypeInformation getProducedType() {
        return returnType;
    }
}
