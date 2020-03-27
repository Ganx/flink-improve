SELECT
    fact2.dim,
    D1.col1 AS A,
    D1.col2 AS B,
    D1.col3 AS C,
    D1.col4 AS D,
    D1.col5 AS E,
    fact2.f_proctime,
    D1.r_proctime
FROM
    fact2,
    LATERAL TABLE (dimension_table2(ff_proctime)) AS D1
WHERE
    fact2.dim     = D1.id
