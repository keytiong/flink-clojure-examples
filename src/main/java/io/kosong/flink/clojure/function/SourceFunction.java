package io.kosong.flink.clojure.function;


import clojure.lang.APersistentMap;
import clojure.lang.IFn;
import clojure.lang.Symbol;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.kosong.flink.clojure.function.Utils.keyword;
import static io.kosong.flink.clojure.function.Utils.resolve;

public class SourceFunction<OUT> extends org.apache.flink.streaming.api.functions.source.RichSourceFunction
        implements ResultTypeQueryable, CheckpointedFunction {

    private static final Logger log = LogManager.getLogger(SourceFunction.class);

    private TypeInformation returnType;

    private Symbol openFnSymbol;
    private Symbol initFnSymbol;
    private Symbol closeFnSymbol;
    private Symbol cancelFnSymbol;
    private Symbol runFnSymbol;
    private Symbol initializeStateFnSymbol;
    private Symbol snapshotStateFnSymbol;

    private transient boolean initialize = false;
    private transient Object state;

    private transient IFn initFn = null;
    private transient IFn openFn = null;
    private transient IFn closeFn = null;
    private transient IFn runFn = null;
    private transient IFn cancelFn = null;
    private transient IFn initalizeStateFn = null;
    private transient IFn snapshotStateFn = null;

    public SourceFunction(APersistentMap args) {
        initFnSymbol = (Symbol) args.invoke(keyword("init"));
        openFnSymbol = (Symbol) args.invoke(keyword("open"));
        closeFnSymbol = (Symbol) args.invoke(keyword("close"));
        runFnSymbol = (Symbol) args.invoke(keyword("run"));
        cancelFnSymbol = (Symbol) args.invoke(keyword("cancel"));
        initializeStateFnSymbol = (Symbol) args.invoke(keyword("initializeState"));
        snapshotStateFnSymbol = (Symbol) args.invoke(keyword("snapshotState"));

        returnType = (TypeInformation) args.invoke(keyword("returns"));
    }

    private void init() {
        initFn = (IFn) resolve(initFnSymbol);
        openFn = (IFn) resolve(openFnSymbol);
        closeFn = (IFn) resolve(closeFnSymbol);
        runFn = (IFn) resolve(runFnSymbol);
        cancelFn = (IFn) resolve(cancelFnSymbol);
        initalizeStateFn = (IFn) resolve(initializeStateFnSymbol);
        snapshotStateFn = (IFn) resolve(snapshotStateFnSymbol);

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

    public Object state() {
        return this.state;
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        if (runFn == null) {
            throw new IllegalStateException("run function is null");
        }
        runFn.invoke(this, ctx);
    }

    @Override
    public void cancel() {
        if (cancelFn != null) {
            cancelFn.invoke(this);
        }
    }

    @Override
    public TypeInformation getProducedType() {
        return returnType;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (snapshotStateFn != null) {
            snapshotStateFn.invoke(this, context);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        if (!initialize) {
            init();
        }

        if (initalizeStateFn != null) {
            initalizeStateFn.invoke(this, context);
        }
    }
}
