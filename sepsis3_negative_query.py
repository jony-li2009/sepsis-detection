from typing import List

def negative_signal_query(sepsis3_table:str,
                          specimens: List[str],
                          icu_name: str,
                          arterial_items: List[str],
                          cuff_items: List[str],
                          max_value: int,                    
                          min_value: int=0,                          
                          lookback: int=12, #get all the data points before the onset time
                          bin_size: int=15):
    """_summary_

    Args:
        sepsis3_table (str): _description_
        icu_name (str): _description_
        arterial_items (List[str]): _description_
        cuff_items (List[str]): _description_
        max_value (int): _description_
        min_value (int, optional): _description_. Defaults to 0.
        lookback (int, optional): _description_. Defaults to 12.
        bin_size (int, optional): _description_. Defaults to 15.
    """
    def sql_in_list(lst):
        if not lst:  # empty list
            return "NULL"  # or "0" if you know IDs are positive
        return ", ".join(map(str, lst))
    
    query = f"""
        WITH
        all_stays AS (
            SELECT stay_id, subject_id, intime, outtime
            FROM `{icu_name}.icustays`
        ),
        positive_stays AS (
            SELECT DISTINCT stay_id
            FROM `{sepsis3_table}`
            WHERE specimen IN UNNEST({specimens})
        ),
        negative_stays AS (
            SELECT a.stay_id, a.intime AS reference_time
            FROM all_stays a
            LEFT JOIN positive_stays p
                ON a.stay_id = p.stay_id
            WHERE p.stay_id IS NULL
            AND TIMESTAMP_DIFF(a.outtime, a.intime, MINUTE) >= 180 -- at least 3 hours stay
        ),
        vital_events AS (
            -- ---------------------------------
            -- Raw vital events for negative stays
            -- ---------------------------------
            SELECT
                SAFE_CAST(n.stay_id AS INT64) AS stay_id,
                n.reference_time AS onset_time,   -- pseudo-onset for negative stays
                ce.charttime,
                ce.valuenum,

                -- 15-min onset-aligned bin index
                FLOOR(
                    TIMESTAMP_DIFF(
                        TIMESTAMP(ce.charttime),
                        TIMESTAMP(n.reference_time),
                        MINUTE
                    )/{bin_size}
                ) AS bin_index,

                CASE
                    WHEN ce.itemid IN ({sql_in_list(arterial_items)}) THEN 'arterial'
                    WHEN ce.itemid IN ({sql_in_list(cuff_items)}) THEN 'cuff'
                END AS modality
            FROM negative_stays n
            JOIN `{icu_name}.chartevents` ce
                ON n.stay_id = ce.stay_id
            WHERE ce.itemid IN ({sql_in_list(arterial_items + cuff_items)})
                AND ce.valuenum IS NOT NULL
                AND ce.valuenum BETWEEN {min_value} AND {max_value}
                
        ),
        first_vital_time AS (
            -- ---------------------------------
            -- First available SBP per stay
            -- ---------------------------------
            SELECT
                stay_id,
                MIN(charttime) AS first_data_time
            FROM vital_events
            GROUP BY stay_id
        ),
        restricted AS (
            -- ---------------------------------
            -- Apply actual_start logic
            -- ---------------------------------
            SELECT
                e.*
            FROM vital_events e
            JOIN first_vital_time f
                ON e.stay_id = f.stay_id            
        ),
        vital_modality_bins AS (
            -- ---------------------------------
            -- Aggregate per bin & modality
            -- ---------------------------------
            SELECT
                stay_id,
                bin_index,
                modality,

                COUNT(*) AS count_val,
                MIN(valuenum) AS min_val,
                MAX(valuenum) AS max_val,
                AVG(valuenum) AS mean_val,
                STDDEV(valuenum) AS std_val,

                ARRAY_AGG(valuenum ORDER BY charttime ASC  LIMIT 1)[OFFSET(0)] AS first_val,
                ARRAY_AGG(valuenum ORDER BY charttime DESC LIMIT 1)[OFFSET(0)] AS last_val,

                ARRAY_AGG(charttime ORDER BY charttime ASC  LIMIT 1)[OFFSET(0)] AS first_time,
                ARRAY_AGG(charttime ORDER BY charttime DESC LIMIT 1)[OFFSET(0)] AS last_time
            FROM restricted
            GROUP BY stay_id, bin_index, modality
        ),
        vital_fused_bins AS (
            -- ---------------------------------
            -- Option A: rank-and-pick fusion
            -- ---------------------------------
            SELECT
                stay_id,
                bin_index,

                ARRAY_AGG(
                    STRUCT(
                        min_val,
                        max_val,
                        mean_val,
                        std_val,
                        count_val,
                        first_val,
                        last_val,
                        first_time,
                        last_time,
                        modality
                    )
                    ORDER BY
                        CASE modality
                            WHEN 'arterial' THEN 1
                            WHEN 'cuff'     THEN 2
                        END
                    LIMIT 1
                )[OFFSET(0)] AS vital
            FROM vital_modality_bins
            GROUP BY stay_id, bin_index
        ),
        final AS (
            -- ---------------------------------
            -- Derived features + labels
            -- ---------------------------------
            SELECT
                stay_id,
                bin_index,

                vital.min_val,
                vital.max_val,
                vital.mean_val,
                vital.std_val,
                vital.count_val,
                vital.first_val,
                vital.last_val,

                SAFE_DIVIDE(
                    vital.last_val - vital.first_val,
                    TIMESTAMP_DIFF(TIMESTAMP(vital.last_time), TIMESTAMP(vital.first_time), SECOND)
                ) AS slope,

                TIMESTAMP_DIFF(TIMESTAMP(vital.last_time), TIMESTAMP(onset_time), MINUTE) AS minutes_from_onset,

                CASE vital.modality
                    WHEN 'arterial' THEN 1
                    WHEN 'cuff'     THEN 2
                END AS vital_source_type,

                LAG(vital.last_val) OVER (PARTITION BY stay_id ORDER BY bin_index) AS prev_last_val,
                vital.last_val - LAG(vital.last_val) OVER (PARTITION BY stay_id ORDER BY bin_index) AS delta_last,
                0 AS label
            FROM vital_fused_bins
            JOIN (
                SELECT DISTINCT stay_id, onset_time
                FROM restricted
            ) USING (stay_id)
        )
        SELECT *
        FROM final
        WHERE minutes_from_onset >= 0 
        ORDER BY stay_id, bin_index;
    """
    
    return query


def negative_temp_query(sepsis3_table:str,
                          specimens: List[str],
                          icu_name: str,                                  
                          bin_size: int=15):
    """_summary_

    Args:
        sepsis3_table (str): _description_
        specimens (List[str]): _description_
        icu_name (str): _description_
        bin_size (int, optional): _description_. Defaults to 15.
    """        
    query = f"""
        WITH
        all_stays AS (
            SELECT stay_id, subject_id, intime, outtime
            FROM `{icu_name}.icustays`
        ),
        positive_stays AS (
            SELECT DISTINCT stay_id
            FROM `{sepsis3_table}`
            WHERE specimen IN UNNEST({specimens})
        ),
        negative_stays AS (
            SELECT a.stay_id, a.intime AS reference_time
            FROM all_stays a
            LEFT JOIN positive_stays p
                ON a.stay_id = p.stay_id
            WHERE p.stay_id IS NULL
            AND TIMESTAMP_DIFF(a.outtime, a.intime, MINUTE) >= 180 -- at least 3 hours stay
        ),
        vital_events AS (
            -- ---------------------------------
            -- Raw vital events for negative stays
            -- ---------------------------------
            SELECT
                SAFE_CAST(n.stay_id AS INT64) AS stay_id,
                n.reference_time AS onset_time,   -- pseudo-onset for negative stays
                ce.charttime,
                CASE
                    WHEN ce.itemid = 223762 THEN ce.valuenum
                    WHEN ce.itemid = 226329 THEN ce.valuenum
                    WHEN ce.itemid = 223761 THEN (ce.valuenum - 32) * 5 / 9
                END AS valuenum,

                -- 15-min onset-aligned bin index
                FLOOR(
                    TIMESTAMP_DIFF(
                        TIMESTAMP(ce.charttime),
                        TIMESTAMP(n.reference_time),
                        MINUTE
                    )/{bin_size}
                ) AS bin_index,

                CASE
                    WHEN ce.itemid = 226329 THEN 'blood'
                    WHEN ce.itemid = 223762 THEN 'tempc'
                    WHEN ce.itemid = 223761 THEN 'tempf'
                END AS modality
                
            FROM negative_stays n
            JOIN `{icu_name}.chartevents` ce
                ON n.stay_id = ce.stay_id
            WHERE ce.valuenum IS NOT NULL
                AND ((ce.itemid IN (223762, 226329) AND ce.valuenum BETWEEN 10 AND 50)
                    OR
                    (ce.itemid = 223761 AND ce.valuenum BETWEEN 70 AND 120)
                )                
        ),
        first_vital_time AS (
            -- ---------------------------------
            -- First available SBP per stay
            -- ---------------------------------
            SELECT
                stay_id,
                MIN(charttime) AS first_data_time
            FROM vital_events
            GROUP BY stay_id
        ),
        restricted AS (
            -- ---------------------------------
            -- Apply actual_start logic
            -- ---------------------------------
            SELECT
                e.*
            FROM vital_events e
            JOIN first_vital_time f
                ON e.stay_id = f.stay_id            
        ),
        vital_modality_bins AS (
            -- ---------------------------------
            -- Aggregate per bin & modality
            -- ---------------------------------
            SELECT
                stay_id,
                bin_index,
                modality,

                COUNT(*) AS count_val,
                MIN(valuenum) AS min_val,
                MAX(valuenum) AS max_val,
                AVG(valuenum) AS mean_val,
                STDDEV(valuenum) AS std_val,

                ARRAY_AGG(valuenum ORDER BY charttime ASC  LIMIT 1)[OFFSET(0)] AS first_val,
                ARRAY_AGG(valuenum ORDER BY charttime DESC LIMIT 1)[OFFSET(0)] AS last_val,

                ARRAY_AGG(charttime ORDER BY charttime ASC  LIMIT 1)[OFFSET(0)] AS first_time,
                ARRAY_AGG(charttime ORDER BY charttime DESC LIMIT 1)[OFFSET(0)] AS last_time
            FROM restricted
            GROUP BY stay_id, bin_index, modality
        ),
        vital_fused_bins AS (
            -- ---------------------------------
            -- Option A: rank-and-pick fusion
            -- ---------------------------------
            SELECT
                stay_id,
                bin_index,

                ARRAY_AGG(
                    STRUCT(
                        min_val,
                        max_val,
                        mean_val,
                        std_val,
                        count_val,
                        first_val,
                        last_val,
                        first_time,
                        last_time,
                        modality
                    )
                    ORDER BY
                        CASE modality
                            WHEN 'blood' THEN 1
                            WHEN 'tempc' THEN 2
                            WHEN 'tempf' THEN 3
                        END
                    LIMIT 1
                )[OFFSET(0)] AS vital
            FROM vital_modality_bins
            GROUP BY stay_id, bin_index
        ),
        final AS (
            -- ---------------------------------
            -- Derived features + labels
            -- ---------------------------------
            SELECT
                stay_id,
                bin_index,

                vital.min_val,
                vital.max_val,
                vital.mean_val,
                vital.std_val,
                vital.count_val,
                vital.first_val,
                vital.last_val,

                SAFE_DIVIDE(
                    vital.last_val - vital.first_val,
                    TIMESTAMP_DIFF(TIMESTAMP(vital.last_time), TIMESTAMP(vital.first_time), SECOND)
                ) AS slope,

                TIMESTAMP_DIFF(TIMESTAMP(vital.last_time), TIMESTAMP(onset_time), MINUTE) AS minutes_from_onset,

                CASE vital.modality
                    WHEN 'blood' THEN 1
                    WHEN 'tempc' THEN 2
                    WHEN 'tempf' THEN 3
                END AS vital_source_type,

                LAG(vital.last_val) OVER (PARTITION BY stay_id ORDER BY bin_index) AS prev_last_val,
                vital.last_val - LAG(vital.last_val) OVER (PARTITION BY stay_id ORDER BY bin_index) AS delta_last,
                0 AS label
            FROM vital_fused_bins
            JOIN (
                SELECT DISTINCT stay_id, onset_time
                FROM restricted
            ) USING (stay_id)
        )
        SELECT *
        FROM final
        WHERE minutes_from_onset >= 0 
        ORDER BY stay_id, bin_index;
    """
    
    return query