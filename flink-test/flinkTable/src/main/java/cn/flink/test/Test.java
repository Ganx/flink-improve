package cn.flink.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * 应用模块名称.
 * <p>
 * 代码描述
 * </P>
 *
 * @author xx
 * @since 2020/3/17 15:45
 */
public class Test {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //对象重用
        env.getConfig().enableObjectReuse();

        StreamTableEnvironment tEnv = StreamTableEnvironment
                .create(env, EnvironmentSettings.newInstance().useBlinkPlanner().build());
    }
}
