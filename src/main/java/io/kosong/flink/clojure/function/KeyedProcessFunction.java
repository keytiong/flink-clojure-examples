package io.kosong.flink.clojure.function;


import clojure.lang.APersistentMap;
import clojure.lang.IFn;
import clojure.lang.Symbol;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.kosong.flink.clojure.function.Utils.keyword;
import static io.kosong.flink.clojure.function.Utils.resolve;

public class KeyedProcessFunction<I, O> extends org.apache.flink.streaming.api.functions.KeyedProcessFunction
        implements ResultTypeQueryable<O> {

    private static final Logger log = LogManager.getLogger(KeyedProcessFunction.class);

    private final TypeInformation returnType;

    private final Symbol openFnSymbol;
    private final Symbol initFnSymbol;
    private final Symbol closeFnSymbol;
    private final Symbol onTimerFnSymbol;
    private final Symbol processElementFnSymbol;

    private transient Object state;
    private transient boolean initialized;

    private transient IFn openFn;
    private transient IFn closeFn;
    private transient IFn onTimerFn;
    private transient IFn processElementFn;
    private transient IFn initFn;

    public KeyedProcessFunction(APersistentMap args) {
        initFnSymbol = (Symbol) args.invoke(keyword("init"));
        openFnSymbol = (Symbol) args.invoke(keyword("open"));
        closeFnSymbol = (Symbol) args.invoke(keyword("close"));
        processElementFnSymbol = (Symbol) args.invoke(keyword("processElement"));
        onTimerFnSymbol = (Symbol) args.invoke(keyword("onTimer"));
        returnType = (TypeInformation) args.invoke(keyword("returns"));
    }

    public Object state() {
        return this.state;
    }

    private void init() {
        openFn = (IFn) resolve(openFnSymbol);
        closeFn = (IFn) resolve(closeFnSymbol);
        onTimerFn = (IFn) resolve(onTimerFnSymbol);
        processElementFn = (IFn) resolve(processElementFnSymbol);
        initFn = (IFn) resolve(initFnSymbol);

        if (initFn != null) {
            this.state = initFn.invoke(this);
        }

        initialized = true;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        if (!initialized) {
            init();
        }

        if (openFn != null) {
            openFn.invoke(this, parameters);
        }
    }

    @Override
    public void close() throws Exception {
        if (closeFn != null) {
            closeFn.invoke(this);
        }
    }

    @Override
    public void processElement(Object value, Context ctx, Collector out) throws Exception {
        processElementFn.invoke(this, value, ctx, out);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector out) throws Exception {
        onTimerFn.invoke(this, timestamp, ctx, out);
    }

    @Override
    public TypeInformation getProducedType() {
        return returnType;
    }
}
