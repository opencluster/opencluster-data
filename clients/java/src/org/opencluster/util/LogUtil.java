package org.opencluster.util;

import org.apache.commons.lang3.exception.ExceptionUtils;
import sun.util.logging.LoggingSupport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Enumeration;
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
        log.log(level, message);
        if (t != null) {
            log.log(level, ExceptionUtils.getMessage(t));
            log.log(level, ExceptionUtils.getStackTrace(t));
            if (ExceptionUtils.getRootCause(t) != null) {
                log.log(level, ExceptionUtils.getRootCauseMessage(t));
                log.log(level, ExceptionUtils.getStackTrace(ExceptionUtils.getRootCause(t)));
            }
        }
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
