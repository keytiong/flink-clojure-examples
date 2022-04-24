package io.kosong.flink.clojure.function;


import clojure.java.api.Clojure;
import clojure.lang.APersistentMap;
import clojure.lang.IFn;
import clojure.lang.Symbol;
import org.apache.flink.configuration.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.kosong.flink.clojure.function.Utils.keyword;
import static io.kosong.flink.clojure.function.Utils.resolve;

public class SinkFunction<IN> extends org.apache.flink.streaming.api.functions.sink.RichSinkFunction{

    private static final Logger log = LogManager.getLogger(ProcessFunction.class);

    private final Symbol openFnSymbol;
    private final Symbol initFnSymbol;
    private final Symbol closeFnSymbol;
    private final Symbol invokeFnSymbol;

    private transient boolean initialize = false;
    private transient Object state;

    private transient IFn initFn = null;
    private transient IFn openFn = null;
    private transient IFn closeFn = null;
    private transient IFn invokeFn = null;


    public SinkFunction(APersistentMap args) {
        initFnSymbol = (Symbol) args.invoke(keyword("init"));
        openFnSymbol = (Symbol) args.invoke(keyword("open"));
        closeFnSymbol = (Symbol) args.invoke(keyword("close"));
        invokeFnSymbol = (Symbol) args.invoke(keyword("invoke"));
    }
    private void init() {
        openFn = (IFn) resolve(openFnSymbol);
        closeFn = (IFn) resolve(closeFnSymbol);
        invokeFn = (IFn) resolve(invokeFnSymbol);
        initFn = (IFn) resolve(initFnSymbol);

        if (initFn != null) {
            state = initFn.invoke(this);
        }

        initialize = true;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (!initialize) {
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
    public void invoke(Object value, Context context) throws Exception {
        invokeFn.invoke(this, value, context);
    }

    public Object state() {
        return this.state;
    }

}
