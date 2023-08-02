CREATE OR REPLACE TABLE DATASDEINTERNACAOEALTACORRIGIDAS AS  
(SELECT
    COD_CLIENTE,
    CASE
        WHEN DT_INTERNACAO = '.' THEN DT_OCORR_EVENTO
        ELSE DT_INTERNACAO
        END AS DT_INTERNACAO,
    CASE
        WHEN DT_ALTA = '.' THEN DT_OCORR_EVENTO
        ELSE DT_ALTA
        END AS DT_ALTA
FROM RAW_SEGUROS.BASE_SEGUROS);