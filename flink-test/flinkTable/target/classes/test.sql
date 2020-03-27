SELECT
    fact_table.dim1 AS dim,
    D1.col1 AS A,
    D1.col2 AS B,
    D1.col3 AS C,
    D1.col4 AS D,
    D1.col5 AS E,
    D1.r_proctime as f_proctime
FROM
    fact_table,
    LATERAL TABLE (dimension_table1(f_proctime)) AS D1
WHERE
    fact_table.dim1     = D1.id
