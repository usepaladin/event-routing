package util

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase

class TestLogAppender : AppenderBase<ILoggingEvent>() {
    val logs = mutableListOf<ILoggingEvent>()

    override fun stop() {
        logs.clear()
        super.stop()
    }

    override fun append(eventObject: ILoggingEvent?) {
        eventObject?.let {
            logs.add(it)
        }
    }

    companion object {
        fun factory(logger: Logger, level: Level = Level.DEBUG): TestLogAppender {
            val appender = TestLogAppender()
            appender.context = logger.loggerContext
            appender.start()
            logger.level = level
            logger.addAppender(appender)
            return appender
        }
    }
}

