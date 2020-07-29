import com.rabbitmq.client.ConfirmCallback
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentNavigableMap
import java.util.concurrent.ConcurrentSkipListMap
import java.util.function.BooleanSupplier

class MainKt {
    companion object RMQ {
    const val MESSAGE_COUNT = 500000

    @Throws(Exception::class)
    fun createConnection(): Connection {
        val cf = ConnectionFactory()
        cf.host = "rabbit-test-srv"
        cf.username = "qa_test"
        cf.password = "qa_test"
        cf.virtualHost = "qa_test_vhost"
        return cf.newConnection()
    }

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        println("AMPQ Publisher Confirm test")
        publishMessagesIndividually()
        publishMessagesInBatch()
        handlePublishConfirmsAsynchronously()
    }

    @Throws(Exception::class)
    fun publishMessagesIndividually() {
        createConnection().use { connection ->
            val ch = connection.createChannel()
            val queue = UUID.randomUUID().toString()
            ch.queueDeclare(queue, false, false, true, null)
            ch.confirmSelect()
            val start = System.nanoTime()
            for (i in 0 until MESSAGE_COUNT) {
                val body = i.toString()
                ch.basicPublish("", queue, null, body.toByteArray())
                ch.waitForConfirmsOrDie(5000)
            }
            val end = System.nanoTime()
            System.out.format(
                "Published %,d messages individually in %,d ms%n",
                MESSAGE_COUNT,
                Duration.ofNanos(end - start).toMillis()
            )
        }
    }

    @Throws(Exception::class)
    fun publishMessagesInBatch() {
        createConnection().use { connection ->
            val ch = connection.createChannel()
            val queue = UUID.randomUUID().toString()
            ch.queueDeclare(queue, false, false, true, null)
            ch.confirmSelect()
            val batchSize = 100
            var outstandingMessageCount = 0
            val start = System.nanoTime()
            for (i in 0 until MESSAGE_COUNT) {
                val body = i.toString()
                ch.basicPublish("", queue, null, body.toByteArray())
                outstandingMessageCount++
                if (outstandingMessageCount == batchSize) {
                    ch.waitForConfirmsOrDie(5000)
                    outstandingMessageCount = 0
                }
            }
            if (outstandingMessageCount > 0) {
                ch.waitForConfirmsOrDie(5000)
            }
            val end = System.nanoTime()
            System.out.format(
                "Published %,d messages in batch in %,d ms%n",
                MESSAGE_COUNT,
                Duration.ofNanos(end - start).toMillis()
            )
        }
    }

    @Throws(Exception::class)
    fun handlePublishConfirmsAsynchronously() {
        createConnection().use { connection ->
            val ch = connection.createChannel()
            val queue = UUID.randomUUID().toString()
            ch.queueDeclare(queue, false, false, true, null)
            ch.confirmSelect()
            val outstandingConfirms: ConcurrentNavigableMap<Long, String> =
                ConcurrentSkipListMap()
            val cleanOutstandingConfirms = ConfirmCallback { sequenceNumber: Long, multiple: Boolean ->
                if (multiple) {
                    val confirmed = outstandingConfirms.headMap(
                        sequenceNumber, true
                    )
                    confirmed.clear()
                } else {
                    outstandingConfirms.remove(sequenceNumber)
                }
            }
            ch.addConfirmListener(cleanOutstandingConfirms, ConfirmCallback { sequenceNumber: Long, multiple: Boolean ->
                val body = outstandingConfirms[sequenceNumber]
                System.err.format(
                    "Message with body %s has been nack-ed. Sequence number: %d, multiple: %b%n",
                    body, sequenceNumber, multiple
                )
                cleanOutstandingConfirms.handle(sequenceNumber, multiple)
            })
            val start = System.nanoTime()
            for (i in 0 until MESSAGE_COUNT) {
                val body = i.toString()
                outstandingConfirms[ch.nextPublishSeqNo] = body
                ch.basicPublish("", queue, null, body.toByteArray())
            }
            check(waitUntil(
                Duration.ofSeconds(60),
                BooleanSupplier { outstandingConfirms.isEmpty() }
            )) { "All messages could not be confirmed in 60 seconds" }
            val end = System.nanoTime()
            System.out.format(
                "Published %,d messages and handled confirms asynchronously in %,d ms%n",
                MESSAGE_COUNT,
                Duration.ofNanos(end - start).toMillis()
            )
        }
    }

    @Throws(InterruptedException::class)
    fun waitUntil(timeout: Duration, condition: BooleanSupplier): Boolean {
        var waited = 0
        while (!condition.asBoolean && waited < timeout.toMillis()) {
            Thread.sleep(1000L)
            waited = +100
        }
        return condition.asBoolean
    }
}
}

