/**
 * Created by rabaner on 3/16/2017.
 */


import java.io.IOException;

import java.nio.charset.*;

import java.time.Instant;

import java.util.*;

import java.util.concurrent.ExecutionException;



import com.google.gson.*;

import com.microsoft.azure.eventhubs.*;

import com.microsoft.azure.servicebus.*;



public class eventhubsender

{

    public static void main(String[] args)

            throws ServiceBusException, ExecutionException, InterruptedException, IOException

    {

        final String namespaceName = "stresstest";

        final String eventHubName = "stresstestentity";

        final String sasKeyName = "writer";

        final String sasKey = "QRybI4S1iBdrOnKq2lwrpriXzgfsBAUq8k6CjlR/fG4=";

        ConnectionStringBuilder connStr = new ConnectionStringBuilder(namespaceName, eventHubName, sasKeyName, sasKey);
        Gson gson = new GsonBuilder().create();
        int count =1;
       while (true){
           if(count==99) {
               count = 1;
           }
            PayloadEvent payload = new PayloadEvent(count%100);
            count++;
            byte[] payloadBytes = gson.toJson(payload).getBytes(Charset.defaultCharset());
            System.out.println("Bytes sent are"+new String(payloadBytes,Charset.defaultCharset()));
            EventData sendEvent = new EventData(payloadBytes);
            EventHubClient ehClient = EventHubClient.createFromConnectionString(connStr.toString()).get();
            PartitionSender sender = ehClient.createPartitionSender("0").get();
            sender.send(sendEvent).get();
            System.out.println(Instant.now() + ": Send Complete...");
            /*System.out.println(String.format("Offset: %s, SeqNo: %s, EnqueueTime: %s",
                    sendEvent.getSystemProperties().getOffset(),
                    sendEvent.getSystemProperties().getSequenceNumber(),
                    sendEvent.getSystemProperties().getEnqueuedTime()));*/
            //Thread.sleep(3000000);
        }
    }



    /**

     * actual application-payload, ex: a telemetry event

     */

    static final class PayloadEvent

    {

        PayloadEvent(final int seed)

        {

            this.id = "telemetryEvent1-critical-eventid-" + seed;

            this.strProperty = "Sample payload"+
            Long.toString(Instant.now().toEpochMilli());

            this.longProperty = seed * new Random().nextInt(seed);

            this.intProperty = seed * new Random().nextInt(seed);

        }
        public String id;
        public String strProperty;
        public long longProperty;
        public int intProperty;

    }

}