package cn.flink.test;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class TestUtils {
    static final int MAX_PARALLELISM = 10;

    static OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> createRescalingTestSink(
            File outDir,
            int totalParallelism,
            int taskIdx,
            long inactivityInterval,
            long partMaxSize) throws Exception {
        System.out.println(outDir.toURI());

        final  RollingPolicy<Tuple2<String,Integer>, String> rollingPolicy =
                DefaultRollingPolicy.create()
                .withMaxPartSize(partMaxSize)
                .withRolloverInterval(inactivityInterval)
                .withInactivityInterval(inactivityInterval)
                .build();

        final BucketAssigner<Tuple2<String,Integer>, String> bucketer = new TupleToStringBucketer();

        final Encoder<Tuple2<String, Integer>> encoder = new Encoder<Tuple2<String, Integer>>() {
            public void encode(Tuple2<String, Integer> stringIntegerTuple2, OutputStream outputStream) throws IOException {
                outputStream.write((stringIntegerTuple2.f0 + '@' + stringIntegerTuple2.f1).getBytes(StandardCharsets.UTF_8));
                outputStream.write('\n');
            }
        };

        return createCustomRescalingTestSink(
                outDir,
                totalParallelism,
                taskIdx,
                10L,
                bucketer,
                encoder,
                rollingPolicy
        );
    }

    static OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> createCustomRescalingTestSink(
            final File outDir,
            final int totalParallelism,
            final int taskIdx,
            final long bucketCheckInterval,
            final BucketAssigner<Tuple2<String, Integer>, String> bucketer,
            final Encoder<Tuple2<String, Integer>> writer,
            final RollingPolicy<Tuple2<String, Integer>, String> rollingPolicy) throws Exception{

        StreamingFileSink<Tuple2<String, Integer>> sink = StreamingFileSink
                .forRowFormat(new Path(outDir.toURI()), writer)
                .withBucketAssigner(bucketer)
                .withRollingPolicy(rollingPolicy)
                .withBucketCheckInterval(bucketCheckInterval)
                .build();
        return new OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object>(new StreamSink<Tuple2<String, Integer>>(sink),MAX_PARALLELISM, totalParallelism, taskIdx);
    }

    static class TupleToStringBucketer implements BucketAssigner<Tuple2<String, Integer>, String> {

        public String getBucketId(Tuple2<String, Integer> stringIntegerTuple2, Context context) {
            //hdfs的分桶路径
            return stringIntegerTuple2.f0;
        }

        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }

    static void checkLocalFs(File outDir, int expectedInProgress, int expectedCompleted){
        int inProgress = 0;
        int finished = 0;
        for (File file: FileUtils.listFiles(outDir, null,true)) {
            if (file.getAbsolutePath().endsWith("crc")) {
                continue;
            }

            if (file.toPath().getFileName().toString().startsWith(".")) {
                inProgress++;
            } else {
                finished++;
            }

            Assert.assertEquals(expectedInProgress, inProgress);
            Assert.assertEquals(expectedCompleted, finished);
        }
    }

    static Map<File, String> getFileContentByPath(File directory) throws IOException {
        Map<File, String> contents = new HashMap<>(4);

        final Collection<File> filesInBucket = FileUtils.listFiles(directory, null, true);
        for (File file : filesInBucket) {
            contents.put(file, FileUtils.readFileToString(file));
        }
        return contents;
    }
}
