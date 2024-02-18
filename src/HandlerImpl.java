import dto.Address;
import dto.Event;
import dto.Payload;
import dto.Result;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.logging.Logger;

public class HandlerImpl implements Handler {

    private final static byte CAPACITY = 100;
    private final static byte NUM_READERS = 10;
    private final static byte NUM_SENDERS = 100;

    private final static String EXCEPTION_MESSAGE = "Не удалось передать данные";

    private Client client;

    public HandlerImpl(Client client) {
        this.client = client;
    }

    @Override
    public void performOperation() {
        var eventsQueue = new ArrayBlockingQueue<Tuple2<Address, Payload>>(CAPACITY);

        var readerExecutorsService = Executors.newFixedThreadPool(NUM_READERS);
        var senderExecutorsService = Executors.newFixedThreadPool(NUM_SENDERS);

        for (int i = 0; i < NUM_READERS; i++) {
            readerExecutorsService.submit(() -> {
                var event = client.readData();
                event.recipients().forEach(recipient -> {
                    try {
                        eventsQueue.put(new Tuple2<>(recipient, event.payload()));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(EXCEPTION_MESSAGE, e);
                    }
                });
            });
        }

        for (int i = 0; i < NUM_SENDERS; i++) {
            senderExecutorsService.submit(() -> {
                try {
                    var tuple2 = eventsQueue.take();
                    var result = client.sendData(tuple2.key(), tuple2.value());
                    if (result.equals(Result.ACCEPTED)) {
                        System.out.println("Произведена успешная отправка");
                    } else {
                        Thread.sleep(timeout().getSeconds());
                        client.sendData(tuple2.key(), tuple2.value());
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(EXCEPTION_MESSAGE, e);
                }
            });
        }

        readerExecutorsService.shutdown();
        senderExecutorsService.shutdown();
    }

    @Override
    public Duration timeout() {
        return Duration.ofSeconds(30);
    }

    private record Tuple2<K, V> (K key, V value) {}
}
