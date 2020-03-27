package com.flink.dim.externalconnectors

/**
  * Connector for dimension table queries.
  *
  */
trait DBConnection extends Serializable{

  import org.apache.flink.types.Row

  /**
    * Initialize connection .
    *
    */
  def open
  /**
    * Batch query dimension table.
    * @throws DimensionJoinException Query Exception
    * @return The result set of a dimension table query
    */
  @throws[DimensionJoinException]
  def batchQuery(): Iterator[Row]
  /**
    * Query dimension table one by one.
    *
    * @throws DimensionJoinException Query Exception
    * @param q Query condition of one by one
    * @return The result of a dimension table query
    */
  @throws[DimensionJoinException]
  def query(q: Row): Row
  /**
    * Release resource.
    *
    */
  def release: Boolean
}
