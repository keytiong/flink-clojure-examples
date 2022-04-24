package io.kosong.flink.clojure.function;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Keyword;

public class Utils {

    private static final IFn RESOLVE_FN;
    private static final IFn REQUIRE_FN;
    private static final IFn KEYWORD_FN;
    private static final IFn NAMESPACE_FN;

    static {
        REQUIRE_FN = Clojure.var("clojure.core/require");
        RESOLVE_FN = Clojure.var("clojure.core/resolve");
        KEYWORD_FN = Clojure.var("clojure.core/keyword");
        NAMESPACE_FN = Clojure.var("clojure.core/namespace");
    }

    public static Object resolve(Object symbol) {
        if (symbol != null) {
            String namespace = (String) NAMESPACE_FN.invoke(symbol);
            if (namespace != null) {
                REQUIRE_FN.invoke(Clojure.read(namespace));
            }
            return RESOLVE_FN.invoke(symbol);
        } else {
            return null;
        }
    }

    public static Keyword keyword(Object keyword) {
        return (Keyword) KEYWORD_FN.invoke(keyword);
    }
}
