package com.instal.beam.lab;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.repackaged.com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.IOChannelFactory;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;

/**
 * Created by paolo on 02/01/17.
 */
public class ProtoInstalMainEventStream {

    private static final Logger LOG = LoggerFactory.getLogger(ProtoInstalMainEventStream.class);
    public static final String BASE_FILE_PATH = "/home/paolo/data/beam/shake";
    public static final String TOPIC = "shakespeare";

    public static class WriteWindowedFilesDoFn
            extends DoFn<KV<IntervalWindow, Iterable<KV<String, String>>>, Void> {

        static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
        static final Coder<String> STRING_CODER = StringUtf8Coder.of();

        private static DateTimeFormatter formatter = ISODateTimeFormat.basicDateTimeNoMillis();

        private final String output;

        public WriteWindowedFilesDoFn(String output) {
            this.output = output;
        }

        @VisibleForTesting
        public static String fileForWindow(String output, IntervalWindow window) {
            return String.format(
                    "%s-%s-%s", output, formatter.print(window.start()), formatter.print(window.end()));
        }

        @ProcessElement
        public void processElement(ProcessContext context) throws Exception {
            // Build a file name from the window
            IntervalWindow window = context.element().getKey();
            String outputShard = fileForWindow(output, window);

            // Open the file and write all the values
            IOChannelFactory factory = IOChannelUtils.getFactory(outputShard);
            LOG.warn("creating file " + outputShard + "...");
            OutputStream out = Channels.newOutputStream(factory.create(outputShard, "text/plain"));
            LOG.warn("created!");
            for (KV<String, String> message :  context.element().getValue()) {
                STRING_CODER.encode(message.getValue(), out, Coder.Context.OUTER);
                out.write(NEWLINE);
            }
            out.close();
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<String, String>> input =
                pipeline.apply("pull-events", KafkaIO.read()
                        .withBootstrapServers("paolo:9092,paolo:9093")
                        .withTopics(ImmutableList.of(TOPIC))
                        .updateConsumerProperties(ImmutableMap.<String, Object>of("group.id", "beam"))
                        .withKeyCoder(StringUtf8Coder.of()) // PCollection<KafkaRecord<Long, byte[]>
                        .withValueCoder(StringUtf8Coder.of())  // PCollection<KafkaRecord<Long, String>
                        .withoutMetadata()

                );

        input
                .apply(Window.<KV<String, String>>into(FixedWindows.of(Duration.standardSeconds(5L)))
                        .withAllowedLateness(Duration.ZERO)
                        .triggering(AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(Duration.standardSeconds(5L)))
                        .discardingFiredPanes()
                    )
                .apply(
                    ParDo.of(
                        new DoFn<KV<String, String>, KV<IntervalWindow, KV<String, String>>>() {
                            @ProcessElement
                            public void processElement(ProcessContext context, IntervalWindow window) {
                                LOG.debug(" >>>>>>>>>>>>>>>>>> Element " + window);
                                LOG.debug("element timestamp {}", context.timestamp());

                                context.output(KV.of(window, context.element()));
                            }
                        }))
                .apply(GroupByKey.<IntervalWindow, KV<String, String>>create())
                .apply(ParDo.of(new WriteWindowedFilesDoFn(BASE_FILE_PATH)));

        pipeline.run().waitUntilFinish();
    }
}
