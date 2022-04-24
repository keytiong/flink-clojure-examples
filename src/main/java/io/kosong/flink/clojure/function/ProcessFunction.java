package io.kosong.flink.clojure.function;


import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ProcessFunction<I, O> extends org.apache.flink.streaming.api.functions.ProcessFunction {

    private static final Logger log = LogManager.getLogger(ProcessFunction.class);

    private final String prefix;
    private final String ns;

    private transient boolean initialize = false;
    private transient Object state;

    private transient IFn openFn = null;
    private transient IFn closeFn = null;
    private transient IFn onTimerFn = null;
    private transient IFn processElementFn = null;

    public ProcessFunction(String ns) {
        this(ns, "-");;
    }

    public ProcessFunction(String ns, String prefix) {
        this.ns = ns;
        this.prefix = prefix;
    }

    private IFn resolveFn(String ns, String name) {
        IFn var = Clojure.var(ns + "/" + name);
        IFn isBoundFn = Clojure.var("clojure.core/bound?");
        Boolean varBound = (Boolean) isBoundFn.invoke(var);

        if (varBound) {
            return var;
        } else {
            return null;
        }
    }

    private void init() {
        Clojure.var("clojure.core/require").invoke(Clojure.read(ns));

        openFn = resolveFn(ns, prefix + "open");
        closeFn = resolveFn(ns, prefix + "close");
        processElementFn = resolveFn(ns, prefix + "processElement");
        onTimerFn = resolveFn(ns, prefix + "onTimer");

        IFn initFn = resolveFn(ns, prefix + "init");
        if (initFn != null) {
            state = initFn.invoke(this);
        }

        initialize = true;
    }

    public Object state() {
        return this.state;
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
    public void processElement(Object value, org.apache.flink.streaming.api.functions.ProcessFunction.Context ctx, Collector out) throws Exception {
        processElementFn.invoke(this, value, ctx, out);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector out) throws Exception {
        onTimerFn.invoke(this, timestamp, ctx, out);
    }

}
