package org.apache.nifi.util

import org.apache.nifi.logging.LogLevel
import org.apache.nifi.processor.Processor
import spock.lang.Specification
import spock.lang.Unroll

class MockComponentLogSpec extends Specification {

    @Unroll
    def "Verify MockComponentLog.#level.(String) logging"() {
        given:
        def uuid = UUID.randomUUID()
        def componentName = "Mock Processor"
        def component = Mock(Processor) {
            toString() >> componentName
        }
        def logger = new MockComponentLog(uuid.toString(), component)

        when:
        logger."$level"(message)
        List<LogMessage> logMessages = logger."get${level.capitalize()}Messages"()

        then:
        logMessages.size() == 1
        def logMessage = logMessages.first()
        logMessage.throwable == null
        logMessage.marker == null
        logMessage.args.length == 1
        logMessage.args[0] == component
        logMessage.msg == expectedFormat


        where:
        level   | message   || expectedFormat
        "trace" | "message" || "{} message"
        "debug" | "message" || "{} message"
        "info"  | "message" || "{} message"
        "warn"  | "message" || "{} message"
        "error" | "message" || "{} message"
    }

    @Unroll
    def "Verify MockComponentLog.#level.(String, Throwable) logging"() {
        given:
        def uuid = UUID.randomUUID()
        def componentName = "Mock Processor"
        def component = Mock(Processor) {
            toString() >> componentName
        }
        def logger = new MockComponentLog(uuid.toString(), component)

        when:
        logger."$level"(message, throwable)
        List<LogMessage> logMessages = logger."get${level.capitalize()}Messages"()

        then:
        logMessages.size() == 1
        def logMessage = logMessages.first()
        logMessage.throwable == throwable
        logMessage.marker == null
        logMessage.args.length == 2
        logMessage.args[0] == component
        logMessage.args[1] == throwable
        logMessage.msg == expectedFormat


        where:
        level   | message   | throwable                           || expectedFormat
        "trace" | "message" | new Throwable("expected throwable") || "{} message"
        "debug" | "message" | new Throwable("expected throwable") || "{} message"
        "info"  | "message" | new Throwable("expected throwable") || "{} message"
        "warn"  | "message" | new Throwable("expected throwable") || "{} message"
        "error" | "message" | new Throwable("expected throwable") || "{} message"
    }

    @Unroll
    def "Verify MockComponentLog.#level.(String, Object) logging"() {
        given:
        def uuid = UUID.randomUUID()
        def componentName = "Mock Processor"
        def component = Mock(Processor) {
            toString() >> componentName
        }
        def logger = new MockComponentLog(uuid.toString(), component)

        when:
        logger."$level"(message, messageParameter)
        List<LogMessage> logMessages = logger."get${level.capitalize()}Messages"()

        then:
        logMessages.size() == 1
        def logMessage = logMessages.first()
        logMessage.throwable == (messageParameter instanceof Throwable ? messageParameter : null)
        logMessage.marker == null
        logMessage.args.length == 2
        logMessage.args[0] == component
        logMessage.args[1] == messageParameter
        logMessage.msg == expectedFormat
        logMessage.formattedMessage == "$component $message"


        where:
        level   | message       | messageParameter                    || expectedFormat | expectedFormattedMessage
//        "trace" | "message: {}" | "parameter"                         || "{} message: {}"
        "debug" | "message: {}" | "parameter"                         || "{} message: {}" | "Mock Processor message: parameter"
//        "info"  | "message: {}" | "parameter"                         || "{} message: {}"
//        "warn"  | "message: {}" | "parameter"                         || "{} message: {}"
//        "error" | "message: {}" | "parameter"                         || "{} message: {}"
//        "trace" | "message: {}" | new Throwable("expected throwable") || "{} message: {}"
        "debug" | "message" | new Throwable("expected throwable") || "{} message" | "Mock Processor message"
//        "info"  | "message: {}" | new Throwable("expected throwable") || "{} message: {}"
//        "warn"  | "message: {}" | new Throwable("expected throwable") || "{} message: {}"
//        "error" | "message: {}" | new Throwable("expected throwable") || "{} message: {}"
    }

    @Unroll
    def "Verify MockComponentLog.#level.(String, Object[]) logging"() {
        given:
        def uuid = UUID.randomUUID()
        def componentName = "Mock Processor"
        def component = Mock(Processor) {
            toString() >> componentName
        }
        def logger = new MockComponentLog(uuid.toString(), component)

        when:
        logger."$level"(message, messageParameters)
        List<LogMessage> logMessages = logger."get${level.capitalize()}Messages"()

        then:
        logMessages.size() == 1
        def logMessage = logMessages.first()
        logMessage.throwable == messageParameters.find { it instanceof Throwable }
        logMessage.marker == null
        logMessage.args.length == [message, messageParameters].flatten().findAll { !(it instanceof Throwable) }.size()
        logMessage.args[0] == component
        logMessage.args[1..-1].containsAll(messageParameters.findAll { !(it instanceof Throwable) })
        logMessage.msg == expectedFormat

        where:
        level   | message           | messageParameters                                                             || expectedFormat
        "trace" | "message: {}"     | ["parameter"] as Object[]                                                     || "{} message: {}"
        "debug" | "message: {}"     | ["parameter"] as Object[]                                                     || "{} message: {}"
        "info"  | "message: {}"     | ["parameter"] as Object[]                                                     || "{} message: {}"
        "warn"  | "message: {}"     | ["parameter"] as Object[]                                                     || "{} message: {}"
        "error" | "message: {}"     | ["parameter"] as Object[]                                                     || "{} message: {}"
        "trace" | "message: {}, {}" | ["parameter1", "parameter2"] as Object[]                                      || "{} message: {}, {}"
        "debug" | "message: {}, {}" | ["parameter1", "parameter2"] as Object[]                                      || "{} message: {}, {}"
        "info"  | "message: {}, {}" | ["parameter1", "parameter2"] as Object[]                                      || "{} message: {}, {}"
        "warn"  | "message: {}, {}" | ["parameter1", "parameter2"] as Object[]                                      || "{} message: {}, {}"
        "error" | "message: {}, {}" | ["parameter1", "parameter2"] as Object[]                                      || "{} message: {}, {}"
        "trace" | "message: {}, {}" | ["parameter1", "parameter2", new Throwable("expected throwable")] as Object[] || "{} message: {}, {}"
        "debug" | "message: {}, {}" | ["parameter1", "parameter2", new Throwable("expected throwable")] as Object[] || "{} message: {}, {}"
        "info"  | "message: {}, {}" | ["parameter1", "parameter2", new Throwable("expected throwable")] as Object[] || "{} message: {}, {}"
        "warn"  | "message: {}, {}" | ["parameter1", "parameter2", new Throwable("expected throwable")] as Object[] || "{} message: {}, {}"
        "error" | "message: {}, {}" | ["parameter1", "parameter2", new Throwable("expected throwable")] as Object[] || "{} message: {}, {}"
    }

    @Unroll
    def "Verify MockComponentLog.log.(LogLevel.#level.toUpperCase(), String) logging"() {
        given:
        def uuid = UUID.randomUUID()
        def componentName = "Mock Processor"
        def component = Mock(Processor) {
            toString() >> componentName
        }
        def logger = new MockComponentLog(uuid.toString(), component)

        when:
        logger.log(LogLevel.valueOf(level.toUpperCase()), message)
        List<LogMessage> logMessages = logger."get${level.capitalize()}Messages"()

        then:
        logMessages.size() == 1
        def logMessage = logMessages.first()
        logMessage.throwable == null
        logMessage.marker == null
        logMessage.args.length == 1
        logMessage.args.first().toString() == componentName
        logMessage.msg == "$componentName $message"


        where:
        level   | message
        "trace" | "trace message"
        "debug" | "debug message"
        "info"  | "info message"
        "warn"  | "warn message"
        "error" | "error message"
    }
}
