package org.opencluster.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.*;

/**
 * User: Brian Gorrie
 * Date: 6/04/12
 * Time: 2:01 PM
 */
public class LogUtil {

    public static void setupDefaultLogLevel(Logger log, Level level) {
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new CustomLogFormatter());

        log.setUseParentHandlers(false);
        log.setLevel(level);
        Handler[] handlers = log.getHandlers();
        for (Handler h : handlers) {
            log.removeHandler(h);
        }
        log.addHandler(handler);

        log.info("Setting logging to log level: " + level + ".");
        Enumeration<String> loggerNames = LogManager.getLogManager().getLoggerNames();
        for (; loggerNames.hasMoreElements(); ) {
            String loggerName = loggerNames.nextElement();
            Logger logger = LogManager.getLogManager().getLogger(loggerName);
            logger.setLevel(level);
            handlers = logger.getHandlers();
            for (Handler h : handlers) {
                logger.removeHandler(h);
            }
            logger.addHandler(handler);
            log.info("[" + loggerName + "] set to " + level);
        }
        log.info("Finished setting logging to log level: " + level + ".");

        log.setUseParentHandlers(true);
    }

    public static void logSevereException(Logger log, String message, Throwable t) {
        logExceptionAtLogLevel(log, Level.SEVERE, message, t);
    }

    public static void logWarningException(Logger log, String message, Throwable t) {
        logExceptionAtLogLevel(log, Level.WARNING, message, t);
    }

    public static void logInfoException(Logger log, String message, Throwable t) {
        logExceptionAtLogLevel(log, Level.INFO, message, t);
    }

    public static void logConfigException(Logger log, String message, Throwable t) {
        logExceptionAtLogLevel(log, Level.CONFIG, message, t);
    }

    public static void logFineException(Logger log, String message, Throwable t) {
        logExceptionAtLogLevel(log, Level.FINE, message, t);
    }

    public static void logFinerException(Logger log, String message, Throwable t) {
        logExceptionAtLogLevel(log, Level.FINER, message, t);
    }

    public static void logFinestException(Logger log, String message, Throwable t) {
        logExceptionAtLogLevel(log, Level.FINEST, message, t);
    }

    public static void logExceptionAtLogLevel(Logger log, Level level, String message, Throwable t) {
        // log the message
        log.log(level, message);
        if (t != null) {

            // log the actual exception
            String shortClassName = getShortClassName(t);
            String throwableMessage = t.getMessage();
            String exceptionMessage = shortClassName + ": " + (throwableMessage == null ? "" : throwableMessage);
            log.log(level, exceptionMessage);
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter, true);
            t.printStackTrace(printWriter);
            log.log(level, stringWriter.getBuffer().toString());

            // dig into the cause
            Throwable throwable = t;
            String[] methodNames = new String[]{
                    "getCause",
                    "getNextException",
                    "getTargetException",
                    "getException",
                    "getSourceException",
                    "getRootCause",
                    "getCausedByException",
                    "getNested",
                    "getLinkedException",
                    "getNestedException",
                    "getLinkedCause",
                    "getThrowable",
            };
            List<Throwable> list = new ArrayList<Throwable>();
            Throwable cause;
            while (throwable != null && !list.contains(throwable)) {
                list.add(throwable);
                cause = null;
                for (int i = 0, methodNamesLength = methodNames.length; cause == null && i < methodNamesLength; i++) {
                    String methodName = methodNames[i];
                    if (methodName != null) {
                        Method method = null;
                        try {
                            method = throwable.getClass().getMethod(methodName);
                        } catch (Throwable t1) {
                            //
                        }

                        if (method != null && Throwable.class.isAssignableFrom(method.getReturnType())) {
                            try {
                                cause = (Throwable) method.invoke(throwable);
                            } catch (Throwable t1) {
                                //
                            }
                        }
                    }
                }
                throwable = cause;
            }
            Throwable rootCause = list.size() < 2 ? null : list.get(list.size() - 1);
            if (rootCause != null) {
                shortClassName = getShortClassName(rootCause);
                throwableMessage = rootCause.getMessage();
                throwableMessage = shortClassName + ": " + (throwableMessage == null ? "" : throwableMessage);
                log.log(level, throwableMessage);
                stringWriter = new StringWriter();
                printWriter = new PrintWriter(stringWriter, true);
                rootCause.printStackTrace(printWriter);
                log.log(level, stringWriter.getBuffer().toString());
            }
        }
    }


    public static String getShortClassName(Object object) {

        Map<String, String> abbreviationToPrimitiveMap = new HashMap<String, String>();

        abbreviationToPrimitiveMap.put("I", "int");
        abbreviationToPrimitiveMap.put("Z", "boolean");
        abbreviationToPrimitiveMap.put("F", "float");
        abbreviationToPrimitiveMap.put("J", "long");
        abbreviationToPrimitiveMap.put("S", "short");
        abbreviationToPrimitiveMap.put("B", "byte");
        abbreviationToPrimitiveMap.put("D", "double");
        abbreviationToPrimitiveMap.put("C", "char");

        String shortClassName = null;
        if (object != null) {
            Class<?> cls = object.getClass();
            shortClassName = "";
            if (cls != null) {
                String className = cls.getName();
                if (className != null && className.length() != 0) {
                    StringBuilder arrayPrefix = new StringBuilder();
                    if (className.startsWith("[")) {
                        while (className.charAt(0) == '[') {
                            className = className.substring(1);
                            arrayPrefix.append("[]");
                        }
                        if (className.charAt(0) == 'L' && className.charAt(className.length() - 1) == ';') {
                            className = className.substring(1, className.length() - 1);
                        }
                    }

                    if (abbreviationToPrimitiveMap.containsKey(className)) {
                        className = abbreviationToPrimitiveMap.get(className);
                    }

                    int lastDotIdx = className.lastIndexOf('.');
                    int innerIdx = className.indexOf(
                            '$', lastDotIdx == -1 ? 0 : lastDotIdx + 1);
                    String out = className.substring(lastDotIdx + 1);
                    if (innerIdx != -1) {
                        out = out.replace('$', '.');
                    }
                    shortClassName = out + arrayPrefix;
                }
            }
        }
        return shortClassName;
    }

    public static void logByteBuffer(Logger log, Level level, ByteBuffer buf) {
        logByteBuffer(log, level, buf, 0, buf.limit());
    }

    public static void logByteBuffer(Logger log, Level level, ByteBuffer buf, int startPos, int endPos) {
        StringBuilder sb = new StringBuilder();
        for (int i = startPos; i < endPos; i++) {
            sb.append("[").append(buf.get(i)).append("]");
        }
        log.log(level, sb.toString());
    }

}
