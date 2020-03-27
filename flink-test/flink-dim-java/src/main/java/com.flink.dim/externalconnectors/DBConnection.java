package com.flink.dim.externalconnectors;

import org.apache.flink.types.Row;
import scala.collection.Iterator;

import java.io.Serializable;

/**
 * 应用模块名称.
 * <p>
 * 代码描述
 * </P>
 *
 * @author xx
 * @since 2020/3/24 14:04
 */
public interface DBConnection extends Serializable {
    public void open();

    public Iterator<Row> batchQuery();

    public Row query(Row q);

    public Boolean release();
}
