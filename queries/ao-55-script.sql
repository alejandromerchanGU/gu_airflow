BEGIN
    LET total_processed := 0; -- Inicializamos el contador de registros procesados
    LET batch_size := 1000; -- Configurar tamaño de lotes
    LET total_rows := 0;

    SELECT COUNT(*)
    INTO total_rows
    FROM DB_GU_DWH.CORE_TABLES.SUPPORTER s
    JOIN DB_GU_SOURCE.PUBLIC.DASH_DONATION_REPORT d
        ON CAST(s.person_id AS STRING) = d."persona_id"
    JOIN DB_GU_DWH.CORE_TABLES.NONPROFITS n
        ON n.nonprofit_id = s.nonprofit_id
    WHERE n.nonprofit_id IS NOT NULL
      AND n.is_active != false
      AND (s.FULL_NAME IS NULL OR LENGTH(TRIM(s.FULL_NAME)) = 0)
      AND IFF(s.DELETED, true, false) <> true
      AND d."persona_id" IS NOT NULL
      AND d."donor_full_name" IS NOT NULL
      AND LENGTH(TRIM(d."donor_full_name")) > 0;

    -- Ciclo WHILE para procesar en lotes
    WHILE (total_processed < total_rows) DO
        -- Actualización de los registros del lote
        UPDATE DB_GU_DWH.CORE_TABLES.SUPPORTER s
        SET
            s.first_name = c."first_name",
            s.last_name = c."last_name",
            s.full_name = LEFT(c."donor_full_name", 100),
            s.updated_at = CURRENT_TIMESTAMP(),
            s.updated_by = 'supporter_setname_task_ao'
        FROM (
            SELECT
                s.supporter_id,
                s.nonprofit_id,
                d."first_name",
                d."last_name",
                d."donor_full_name"
            FROM DB_GU_DWH.CORE_TABLES.SUPPORTER s
            JOIN DB_GU_SOURCE.PUBLIC.DASH_DONATION_REPORT d
                ON CAST(s.person_id AS STRING) = d."persona_id"
            JOIN DB_GU_DWH.CORE_TABLES.NONPROFITS n
                ON n.nonprofit_id = s.nonprofit_id
            WHERE
                n.nonprofit_id IS NOT NULL
                AND n.is_active != false
                AND (s.FULL_NAME IS NULL OR LENGTH(TRIM(s.FULL_NAME)) = 0)
                AND IFF(s.DELETED, true, false) <> true
                AND d."persona_id" IS NOT NULL
                AND d."donor_full_name" IS NOT NULL
                AND LENGTH(TRIM(d."donor_full_name")) > 0
                QUALIFY ROW_NUMBER() OVER (ORDER BY s.supporter_id) BETWEEN 1 AND :batch_size  -- Seleccionamos un lote con el tamaño del batch
        ) c
        WHERE s.supporter_id = c.supporter_id
          AND s.nonprofit_id = c.nonprofit_id
          AND (s.first_name IS DISTINCT FROM c."first_name"
               OR s.last_name IS DISTINCT FROM c."last_name"
               OR s.full_name IS DISTINCT FROM LEFT(c."donor_full_name", 100));

        -- Actualizamos el contador de registros procesados
        total_processed := total_processed + :batch_size;
    END WHILE;
    INSERT INTO DB_GU_DWH.SYNCLOGS.FIX_SUPPORTERS_LOGS (execution_date, rows_updated, process_name) VALUES (CURRENT_DATE, TO_CHAR(:total_rows), 'ao-55-script');
    COMMIT;
    RETURN 'Task supporter_setname_task_ao completed: ' || TO_CHAR(total_rows) || ' rows updated';
END;