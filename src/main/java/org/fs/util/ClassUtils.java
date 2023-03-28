package org.fs.util;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.function.Function;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class ClassUtils {

    public static <T> T getProxyInstance(Class<T> cls) {
        return (T) Proxy.newProxyInstance(cls.getClassLoader(),
                new Class[] { cls },
                new ProxyInvocationHandler());
    }

    public static <T, R> FunctionInfo getFunctionInfo(SerialFunction<T, R> function) {
        try {
            Method writeReplace = function.getClass().getDeclaredMethod("writeReplace");
            writeReplace.setAccessible(true);
            SerializedLambda serializedLambda = (SerializedLambda) writeReplace.invoke(function);
            return FunctionInfo.builder()
                    .className(serializedLambda.getImplClass().replaceAll("/", "."))
                    .methodName(serializedLambda.getImplMethodName())
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("get Functional info error", e);
        }
    }

    public static <T, R> FunctionInfo getFunctionInfo(Class<?> cls, SerialFunction<T, R> function) {
        FunctionInfo functionInfo = getFunctionInfo(function);
        functionInfo.setClassName(cls.getName());
        return functionInfo;
    }

    private ClassUtils() {
    }

    @FunctionalInterface
    public static interface SerialFunction<T, R> extends Function<T, R>, Serializable {
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FunctionInfo {
        private String className;
        private String methodName;
    }

    public static class ProxyInvocationHandler implements InvocationHandler {

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            return "success";
        }

    }
}
