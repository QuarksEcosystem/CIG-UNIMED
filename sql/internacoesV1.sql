SELECT 
    ID_INTERNACAO,
    COD_CLIENTE,
    MIN(DT_INTERNACAO) AS DT_INTERNACAO,
    MAX(DT_ALTA) AS DT_ALTA
FROM
    (SELECT
        C.*,
        SUM(ID_INTERNACAO_PRE) OVER(ORDER BY 
                                COD_CLIENTE DESC,
                                DT_INTERNACAO ASC,
                                DT_ALTA DESC) AS ID_INTERNACAO
    FROM
        (
        SELECT 
            B.*,
            CASE 
                WHEN (PREV_COD_CLIENTE != COD_CLIENTE  OR PREV_COD_CLIENTE = -1)
                     OR
                     (PREV_COD_CLIENTE = COD_CLIENTE AND
                      (DATEADD(day, 1, DT_INTERNACAO) >= PREV_DT_INTERNACAO AND DT_INTERNACAO <= DATEADD(day, -1, PREV_DT_ALTA))
                      OR
                      (DATEADD(day, 1, DT_ALTA) >= PREV_DT_INTERNACAO AND DT_ALTA <= DATEADD(day, -1, PREV_DT_ALTA))
                       OR
                      (DATEADD(day, 1, PREV_DT_INTERNACAO) >= DT_INTERNACAO AND PREV_DT_INTERNACAO <= DATEADD(day, -1,DT_ALTA))
                      OR
                      (DATEADD(day, 1, PREV_DT_ALTA) >= DT_INTERNACAO AND PREV_DT_ALTA <= DATEADD(day, -1, DT_ALTA))
                       )
                         THEN 0
                ELSE 1
            END AS ID_INTERNACAO_PRE
        FROM
            (SELECT 
                A.*,
                LAG(COD_CLIENTE, 1, -1)    OVER(ORDER BY 
                                             COD_CLIENTE DESC,
                                             DT_INTERNACAO ASC,
                                             DT_ALTA DESC) AS PREV_COD_CLIENTE,
                LAG(DT_INTERNACAO, 1)      OVER(ORDER BY 
                                             COD_CLIENTE DESC,
                                             DT_INTERNACAO ASC,
                                             DT_ALTA DESC) AS PREV_DT_INTERNACAO,
                LAG(DT_ALTA, 1)            OVER(ORDER BY 
                                             COD_CLIENTE DESC,
                                             DT_INTERNACAO ASC,
                                             DT_ALTA DESC) AS PREV_DT_ALTA
            FROM (
                    SELECT 
                        NULLIF(COD_CLIENTE, '.') AS COD_CLIENTE,
                        TRY_CAST(DT_INTERNACAO AS date) AS DT_INTERNACAO,
                        TRY_CAST(DT_ALTA AS date) AS DT_ALTA
                    FROM (
                            SELECT * FROM
                                (SELECT 
                                    COD_CLIENTE,
                                    DT_INTERNACAO,
                                    MAX(DT_ALTA) AS DT_ALTA
                                FROM (SELECT
                                        COD_CLIENTE,
                                        CASE
                                            WHEN DT_INTERNACAO = '.' THEN DT_OCORR_EVENTO
                                            ELSE DT_INTERNACAO
                                            END AS DT_INTERNACAO,
                                        CASE
                                            WHEN DT_ALTA = '.' THEN DT_OCORR_EVENTO
                                            ELSE DT_ALTA
                                            END AS DT_ALTA
                                    FROM RAW_SEGUROS.BASE_SEGUROS) GROUP BY COD_CLIENTE, DT_INTERNACAO
                                UNION
                                SELECT 
                                    COD_CLIENTE,
                                    MIN(DT_INTERNACAO),
                                    DT_ALTA AS DT_ALTA
                                FROM (SELECT
                                        COD_CLIENTE,
                                        CASE
                                            WHEN DT_INTERNACAO = '.' THEN DT_OCORR_EVENTO
                                            ELSE DT_INTERNACAO
                                            END AS DT_INTERNACAO,
                                        CASE
                                            WHEN DT_ALTA = '.' THEN DT_OCORR_EVENTO
                                            ELSE DT_ALTA
                                            END AS DT_ALTA
                                    FROM RAW_SEGUROS.BASE_SEGUROS) GROUP BY COD_CLIENTE, DT_ALTA
                                )
                                ORDER BY COD_CLIENTE ASC, DT_INTERNACAO ASC, DT_ALTA DESC
                        )
                ) A
            ) B
        ) C) ID_INT GROUP BY ID_INTERNACAO, COD_CLIENTE
            LIMIT 10000;