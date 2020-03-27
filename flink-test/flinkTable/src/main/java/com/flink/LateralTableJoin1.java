/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flink;

import com.flink.tables.DimensionTable;
import com.flink.tables.FactTable;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.EnvironmentSettings.Builder;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.stream.Collectors;

public class LateralTableJoin1 {

    private static final Logger LOG = LoggerFactory.getLogger(LateralTableJoin1.class);

    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        int factTableRate = tool.getInt("factRate", 500_000);
        int dimTableRate = tool.getInt("dimRate", 1_000);

        StreamExecutionEnvironment env = getEnvironment(tool);
        //Builder plannerBuilder =  EnvironmentSettings.newInstance().useOldPlanner();
        Builder plannerBuilder =  EnvironmentSettings.newInstance().useBlinkPlanner();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, plannerBuilder.build());

        int maxId = 100_000;
        tEnv.registerDataStream(
                "fact_table",
                env.addSource(new FactTable(factTableRate, maxId)),
                "dim1, dim2, dim3, dim4, dim5, f_proctime.proctime");
        tEnv.registerDataStream(
                "dim_table1",
                env.addSource(new DimensionTable(dimTableRate, maxId)),
                "id, col1, col2, col3, col4, col5, r_proctime.proctime");
        tEnv.registerDataStream(
                "dim_table2",
                env.addSource(new DimensionTable(dimTableRate, maxId)),
                "id, col1, col2, col3, col4, col5, r_proctime.proctime");
        tEnv.registerDataStream(
                "dim_table3",
                env.addSource(new DimensionTable(dimTableRate, maxId)),
                "id, col1, col2, col3, col4, col5, r_proctime.proctime");
        tEnv.registerDataStream(
                "dim_table4",
                env.addSource(new DimensionTable(dimTableRate, maxId)),
                "id, col1, col2, col3, col4, col5, r_proctime.proctime");
        tEnv.registerDataStream(
                "dim_table5",
                env.addSource(new DimensionTable(dimTableRate, maxId)),
                "id, col1, col2, col3, col4, col5, r_proctime.proctime");


        tEnv.registerFunction(
                "dimension_table1",
                tEnv.scan("dim_table1").createTemporalTableFunction("r_proctime", "id"));
        tEnv.registerFunction(
                "dimension_table2",
                tEnv.scan("dim_table2").createTemporalTableFunction("r_proctime", "id"));

        String query = loadQuery("test.sql");
        LOG.info(query);

        Table results = tEnv.sqlQuery(query);

       DataStream dataStream = tEnv.toAppendStream(results, Row.class);
        System.out.println("********************************************");
        //dataStream.print();

       tEnv.registerDataStream("fact2", dataStream,"dim,A,B,C,D,E,f_proctime,ff_proctime.proctime");
        String query1 = loadQuery("test1.sql");
        Table results1 = tEnv.sqlQuery(query1);

        DataStream dataStream1 = tEnv.toAppendStream(results1, Row.class);
       dataStream1.print();
        env.execute("test");
    }

    static StreamExecutionEnvironment getEnvironment(ParameterTool tool) {

        final StreamExecutionEnvironment env;

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        return env;
    }

    private static String loadQuery(String sql) throws IOException {
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        try (InputStream is = classLoader.getResourceAsStream(sql)) {
            if (is == null) {
                throw new FileNotFoundException("File 'query.sql' not found");
            }
            try (InputStreamReader isr = new InputStreamReader(is);
                 BufferedReader reader = new BufferedReader(isr)) {
                return reader.lines().collect(Collectors.joining(System.lineSeparator()));
            }
        }
    }
}
