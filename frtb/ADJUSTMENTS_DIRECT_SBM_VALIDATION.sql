CREATE OR REPLACE PROCEDURE ADJUSTMENT.ADJUSTMENTS_DIRECT_SBM_VALIDATION()
RETURNS TABLE (FILE_ROW_NUMBER NUMBER, COLUMN_NAME VARCHAR, ERROR_MESSAGE VARCHAR)
LANGUAGE SQL
COMMENT='Date=20260618 | Author=Eldho | Description=Row-level validation rules for ADJUSTMENTS_DIRECT_SBM'
EXECUTE AS OWNER
AS
$$
DECLARE
    rs RESULTSET DEFAULT (
        WITH base AS (
            SELECT
            FILE_ROW_NUMBER AS FILE_ROW_NUMBER,
            * EXCLUDE (FILE_ROW_NUMBER)
            FROM ADJUSTMENT.ADJUSTMENTS_DIRECT_SBM
            -- NOTE: Rule field EVALUATION_DATE is validated using COBID (derived date in downstream view).
            -- NOTE: Rule field VERTEX_UNDERLYING is validated using UNDERLYING_TENOR_CODE.
            -- NOTE: Rule fields BUSINESS_ORGANIZATION_CODE / BOOK_CODE are derived in the SBM view via BOOK mapping.
            -- MISSING FROM TABLE DDL BUT PRESENT IN THE RULE SHEET:
            --   MUFG_ORGANIZATION_CODE
            --   EBA_BUCKET, EBA_BUCKET_NAME, FED_BUCKET, FED_BUCKET_NAME
            --   PRA_AMOUNT, PRA_AMOUNT_USD, FED_AMOUNT, FED_AMOUNT_USD, EBA_AMOUNT, EBA_AMOUNT_USD
            --   PRA_PV_SIMULATED, EBA_PV_SIMULATED, FED_PV_SIMULATED
            --   EBA_DELTA_SUBTRACT, FED_DELTA_SUBTRACT
            -- These are commented because the base table DDL does not define them.
        ),
        validation_errors AS (
            SELECT FILE_ROW_NUMBER, 'COBID' AS COLUMN_NAME, 'Error: EVALUATION_DATE (COBID) is required' AS ERROR_MESSAGE
            FROM base
            WHERE COBID IS NULL

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'ENTITY_CODE', 'Error: ENTITY_CODE is required'
            FROM base
            WHERE ENTITY_CODE IS NULL OR LENGTH(TRIM(ENTITY_CODE)) = 0

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'MUFG_PRODUCT_CODE', 'Error: MUFG_PRODUCT_CODE is required'
            FROM base
            WHERE MUFG_PRODUCT_CODE IS NULL OR LENGTH(TRIM(MUFG_PRODUCT_CODE)) = 0

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'BUSINESS_PRODUCT_CODE4', 'Error: BUSINESS_PRODUCT_CODE4 (Typology) is required'
            FROM base
            WHERE BUSINESS_PRODUCT_CODE4 IS NULL OR LENGTH(TRIM(BUSINESS_PRODUCT_CODE4)) = 0

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'TRADING_DESK', 'Error: TRADING_DESK is required'
            FROM base
            WHERE TRADING_DESK IS NULL OR LENGTH(TRIM(TRADING_DESK)) = 0

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'SENSITIVITY_TYPE', 'Error: SENSITIVITY_TYPE is required'
            FROM base
            WHERE SENSITIVITY_TYPE IS NULL OR LENGTH(TRIM(SENSITIVITY_TYPE)) = 0

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'RISK_CLASS', 'Error: RISK_CLASS is required'
            FROM base
            WHERE RISK_CLASS IS NULL OR LENGTH(TRIM(RISK_CLASS)) = 0

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'AMOUNT', 'Error: AMOUNT (sensitivity) is required'
            FROM base
            WHERE AMOUNT IS NULL

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'AMOUNT_IN_USD', 'Error: AMOUNT_IN_USD (sensitivity USD) is required'
            FROM base
            WHERE AMOUNT_IN_USD IS NULL

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'CCY1', 'Error: CCY1 is required for FX and GIRR positions'
            FROM base
            WHERE (UPPER(COALESCE(RISK_CLASS, '')) LIKE '%FX%'
                   OR UPPER(COALESCE(RISK_CLASS, '')) LIKE '%GIRR%')
              AND (CCY1 IS NULL OR LENGTH(TRIM(CCY1)) = 0)

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'CCY_AMT', 'Error: CCY_AMT is required for FX and GIRR positions'
            FROM base
            WHERE (UPPER(COALESCE(RISK_CLASS, '')) LIKE '%FX%'
                   OR UPPER(COALESCE(RISK_CLASS, '')) LIKE '%GIRR%')
              AND (CCY_AMT IS NULL OR LENGTH(TRIM(CCY_AMT)) = 0)

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'CCY2', 'Error: CCY2 is required for FX positions'
            FROM base
            WHERE UPPER(COALESCE(RISK_CLASS, '')) LIKE '%FX%'
              AND (CCY2 IS NULL OR LENGTH(TRIM(CCY2)) = 0)

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'MATURITY_OF_THE_OPTION', 'Error: MATURITY_OF_THE_OPTION is required for FX Vega positions'
            FROM base
            WHERE UPPER(COALESCE(RISK_CLASS, '')) LIKE '%FX%'
              AND UPPER(COALESCE(SENSITIVITY_TYPE, '')) LIKE '%VEGA%'
              AND (MATURITY_OF_THE_OPTION IS NULL OR LENGTH(TRIM(MATURITY_OF_THE_OPTION)) = 0)

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'UNDERLYING_TENOR_CODE', 'Error: UNDERLYING_TENOR_CODE (VERTEX_UNDERLYING) is required for FX Vega positions'
            FROM base
            WHERE UPPER(COALESCE(RISK_CLASS, '')) LIKE '%FX%'
              AND UPPER(COALESCE(SENSITIVITY_TYPE, '')) LIKE '%VEGA%'
              AND (UNDERLYING_TENOR_CODE IS NULL OR LENGTH(TRIM(UNDERLYING_TENOR_CODE)) = 0)

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'CURVE_TYPE', 'Error: CURVE_TYPE is required for GIRR Curvature positions'
            FROM base
            WHERE UPPER(COALESCE(RISK_CLASS, '')) LIKE '%GIRR%'
              AND UPPER(COALESCE(SENSITIVITY_TYPE, '')) LIKE '%CURVATURE%'
              AND (CURVE_TYPE IS NULL OR LENGTH(TRIM(CURVE_TYPE)) = 0)

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'VERTEX', 'Error: VERTEX is required for GIRR Delta and CSR Non-Sec Delta positions'
            FROM base
            WHERE (
                    (UPPER(COALESCE(RISK_CLASS, '')) LIKE '%GIRR%'
                     AND UPPER(COALESCE(SENSITIVITY_TYPE, '')) LIKE '%DELTA%')
                    OR
                    (UPPER(COALESCE(RISK_CLASS, '')) LIKE '%CSR%'
                     AND UPPER(COALESCE(RISK_CLASS, '')) NOT LIKE '%SEC%DELTA%'
                     AND UPPER(COALESCE(SENSITIVITY_TYPE, '')) LIKE '%DELTA%')
                  )
              AND (VERTEX IS NULL OR LENGTH(TRIM(VERTEX)) = 0)

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'CURVATURE_SCENARIO', 'Error: CURVATURE_SCENARIO is required for Curvature positions'
            FROM base
            WHERE UPPER(COALESCE(SENSITIVITY_TYPE, '')) LIKE '%CURVATURE%'
              AND (CURVATURE_SCENARIO IS NULL OR LENGTH(TRIM(CURVATURE_SCENARIO)) = 0)

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'BUCKET', 'Error: BUCKET is required for Equity and CSR positions'
            FROM base
            WHERE (UPPER(COALESCE(RISK_CLASS, '')) LIKE '%EQUIT%'
                   OR UPPER(COALESCE(RISK_CLASS, '')) LIKE '%CSR%')
              AND (BUCKET IS NULL OR LENGTH(TRIM(BUCKET)) = 0)

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'SECURITY_CODE_TYPE', 'Error: SECURITY_CODE_TYPE is required for Equity and CSR positions'
            FROM base
            WHERE (UPPER(COALESCE(RISK_CLASS, '')) LIKE '%EQUIT%'
                   OR UPPER(COALESCE(RISK_CLASS, '')) LIKE '%CSR%')
              AND (SECURITY_CODE_TYPE IS NULL OR LENGTH(TRIM(SECURITY_CODE_TYPE)) = 0)

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'SECURITY_CODE', 'Error: SECURITY_CODE is required for Equity and CSR positions'
            FROM base
            WHERE (UPPER(COALESCE(RISK_CLASS, '')) LIKE '%EQUIT%'
                   OR UPPER(COALESCE(RISK_CLASS, '')) LIKE '%CSR%')
              AND (SECURITY_CODE IS NULL OR LENGTH(TRIM(SECURITY_CODE)) = 0)

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'SECURITY_INFORMATION1', 'Error: SECURITY_INFORMATION1 is required for Equity and CSR positions'
            FROM base
            WHERE (UPPER(COALESCE(RISK_CLASS, '')) LIKE '%EQUIT%'
                   OR UPPER(COALESCE(RISK_CLASS, '')) LIKE '%CSR%')
              AND (SECURITY_INFORMATION1 IS NULL OR LENGTH(TRIM(SECURITY_INFORMATION1)) = 0)

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'SECURITY_INFORMATION2', 'Error: SECURITY_INFORMATION2 is required for Equity and CSR positions'
            FROM base
            WHERE (UPPER(COALESCE(RISK_CLASS, '')) LIKE '%EQUIT%'
                   OR UPPER(COALESCE(RISK_CLASS, '')) LIKE '%CSR%')
              AND (SECURITY_INFORMATION2 IS NULL OR LENGTH(TRIM(SECURITY_INFORMATION2)) = 0)

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'ISSUER_CODE', 'Error: ISSUER_CODE is required for Equity and CSR positions'
            FROM base
            WHERE (UPPER(COALESCE(RISK_CLASS, '')) LIKE '%EQUIT%'
                   OR UPPER(COALESCE(RISK_CLASS, '')) LIKE '%CSR%')
              AND (ISSUER_CODE IS NULL OR LENGTH(TRIM(ISSUER_CODE)) = 0)

            UNION ALL
              SELECT FILE_ROW_NUMBER, 'ISSUER_NAME', 'Error: ISSUER_NAME is required for Equity and CSR positions'
              FROM base
              WHERE (UPPER(COALESCE(RISK_CLASS, '')) LIKE '%EQUIT%'
                  OR UPPER(COALESCE(RISK_CLASS, '')) LIKE '%CSR%')
                AND (ISSUER_NAME IS NULL OR LENGTH(TRIM(ISSUER_NAME)) = 0)

              UNION ALL
            SELECT FILE_ROW_NUMBER, 'PRA_BUCKET', 'Error: PRA_BUCKET is required for Equity and CSR positions'
            FROM base
            WHERE (UPPER(COALESCE(RISK_CLASS, '')) LIKE '%EQUIT%'
                   OR UPPER(COALESCE(RISK_CLASS, '')) LIKE '%CSR%')
              AND (PRA_BUCKET IS NULL OR LENGTH(TRIM(PRA_BUCKET)) = 0)

            UNION ALL
            SELECT FILE_ROW_NUMBER, 'SECURITY_INFORMATION3', 'Error: SECURITY_INFORMATION3 (Sector) is required for CSR positions'
            FROM base
            WHERE UPPER(COALESCE(RISK_CLASS, '')) LIKE '%CSR%'
              AND (SECURITY_INFORMATION3 IS NULL OR LENGTH(TRIM(SECURITY_INFORMATION3)) = 0)
        )
        SELECT FILE_ROW_NUMBER, COLUMN_NAME, ERROR_MESSAGE
        FROM validation_errors
        ORDER BY FILE_ROW_NUMBER, COLUMN_NAME
    );
BEGIN
    RETURN TABLE(rs);
END;
$$;