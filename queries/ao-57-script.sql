BEGIN
    LET batch_size := 1000; -- Configurar tamaño de lotes
    LET total_processed := 0; -- Inicializamos el contador de registros procesados
    LET total_rows := 0;

    SELECT COUNT(DISTINCT s.supporter_id)
    INTO total_rows
    FROM DB_GU_DWH.CORE_TABLES.SUPPORTER s
    LEFT JOIN DB_GU_SOURCE.PUBLIC.DASH_DONATION_REPORT dr ON CAST(s.person_id AS STRING) = dr."persona_id"
    INNER JOIN DB_GU_DWH.CORE_TABLES.NONPROFITS n ON n.nonprofit_id = s.nonprofit_id
    WHERE n.nonprofit_id IS NOT NULL
      AND n.is_active != false
      AND (s.FULL_NAME IS NULL OR LENGTH(TRIM(s.FULL_NAME)) = 0)
      AND IFF(s.DELETED, true, false) <> true
      AND (dr."persona_id" IS NULL OR
        (dr."persona_id" IS NOT NULL AND ( dr."donor_full_name" IS NULL OR LENGTH(TRIM(dr."donor_full_name")) = 0)));

    -- Ciclo WHILE para procesar en lotes
    WHILE (total_processed < total_rows) DO
        -- Actualización de los registros del lote
        UPDATE DB_GU_DWH.CORE_TABLES.SUPPORTER s
        SET first_name = CASE
        		WHEN sc.TOTAL_CONTRIBUTION > 0
        			THEN 'No name'
        		ELSE first_name -- Mantener el valor actual si TOTAL_CONTRIBUTION no es mayor a 0
        		END
        	,last_name = CASE
        		WHEN sc.TOTAL_CONTRIBUTION > 0
        			THEN 'identified'
        		ELSE last_name -- Mantener el valor actual si TOTAL_CONTRIBUTION no es mayor a 0
        		END
        	,full_name = CASE
        		WHEN sc.TOTAL_CONTRIBUTION > 0
        			THEN 'No name identified'
        		ELSE full_name -- Mantener el valor actual si TOTAL_CONTRIBUTION no es mayor a 0
        		END
        	,updated_at = CASE
        		WHEN sc.TOTAL_CONTRIBUTION > 0
        			THEN CURRENT_TIMESTAMP()
        		ELSE updated_at -- Mantener el valor actual si TOTAL_CONTRIBUTION no es mayor a 0
        		END
        	,updated_by = CASE
        		WHEN sc.TOTAL_CONTRIBUTION > 0
        			THEN 'supporter_set_defaultname_task_ao'
        		ELSE updated_by -- Mantener el valor actual si TOTAL_CONTRIBUTION no es mayor a 0
        		END
        	,deleted = CASE
        		WHEN sc.TOTAL_CONTRIBUTION = 0
        			THEN true
        		ELSE deleted -- Mantener el valor actual si TOTAL_CONTRIBUTION no es 0
        		END
        	,deleted_at = CASE
        		WHEN sc.TOTAL_CONTRIBUTION = 0
        			THEN CURRENT_TIMESTAMP()
        		ELSE deleted_at -- Mantener el valor actual si TOTAL_CONTRIBUTION no es 0
        		END
        	,deleted_by = CASE
        		WHEN sc.TOTAL_CONTRIBUTION = 0
        			THEN 'supporter_soft_deleted_task_ao'
        		ELSE deleted_by -- Mantener el valor actual si TOTAL_CONTRIBUTION no es 0
        		END
        FROM (
        	SELECT s.SUPPORTER_ID
        		,MAX(s.NONPROFIT_ID) AS NONPROFIT_ID
        		,COALESCE(SUM(IFF(d2.payment_amount IS NOT NULL, d2.payment_amount, 0)), 0) AS FUNDRAISED_SUM
        		,COALESCE(SUM(IFF(d.supporter_id = fa.supporter_id, d.payment_amount, 0)), 0) AS SELF_DONATION_SUM
        		,COALESCE(SUM(d.payment_amount), 0) AS DONATION_SUM
        		,(FUNDRAISED_SUM - SELF_DONATION_SUM) AS NET_FUNDRAISED_SUM
        		,ROUND(NET_FUNDRAISED_SUM + DONATION_SUM) AS TOTAL_CONTRIBUTION
        	FROM DB_GU_DWH.CORE_TABLES.SUPPORTER s
        	LEFT JOIN DB_GU_DWH.CORE_TABLES.DONATION d ON d.supporter_id = s.supporter_id
        		AND d.nonprofit_id = s.nonprofit_id
        	LEFT JOIN DB_GU_DWH.CORE_TABLES.FUNDRAISER fa ON d.fundraiser_id = fa.fundraiser_id
        		AND fa.nonprofit_id = s.nonprofit_id
        	LEFT JOIN DB_GU_DWH.CORE_TABLES.FUNDRAISER fa2 ON fa2.supporter_id = s.supporter_id
        		AND fa2.nonprofit_id = s.nonprofit_id
        	LEFT JOIN DB_GU_SOURCE.PUBLIC.DASH_DONATION_REPORT dr ON CAST(s.person_id AS STRING) = dr."persona_id"
        	LEFT JOIN DB_GU_DWH.CORE_TABLES.DONATION d2 ON d2.fundraiser_id = fa2.fundraiser_id
        		AND d2.nonprofit_id = s.nonprofit_id
        	INNER JOIN DB_GU_DWH.CORE_TABLES.NONPROFITS n ON n.nonprofit_id = s.nonprofit_id
        	WHERE n.nonprofit_id IS NOT NULL
        		AND n.is_active != false
        		AND (s.FULL_NAME IS NULL OR LENGTH(TRIM(s.FULL_NAME)) = 0)
        		AND IFF(s.DELETED, true, false) <> true
        		AND (dr."persona_id" IS NULL OR
                    (dr."persona_id" IS NOT NULL AND (dr."donor_full_name" IS NULL OR LENGTH(TRIM(dr."donor_full_name")) = 0)))
        	GROUP BY s.supporter_id
            QUALIFY ROW_NUMBER() OVER (ORDER BY s.supporter_id) BETWEEN 1 AND :batch_size  -- Seleccionamos un lote con el tamaño del batch
        	) sc
        WHERE s.supporter_id = sc.SUPPORTER_ID
        	AND s.nonprofit_id = sc.NONPROFIT_ID;

        -- Actualizamos el contador de registros procesados
        total_processed := total_processed + :batch_size;
    END WHILE;
    INSERT INTO DB_GU_DWH.SYNCLOGS.FIX_SUPPORTERS_LOGS (execution_date, rows_updated, process_name) VALUES (CURRENT_DATE, TO_CHAR(:total_rows), 'ao-57-script');
    COMMIT;
    RETURN 'Task supporter_set_defaultname_task_ao and supporter_soft_deleted_task_ao completed: ' || TO_CHAR(total_rows) || ' rows updated';
END;
