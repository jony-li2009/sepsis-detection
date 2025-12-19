from typing import List

def positive_query(sepsis3_table:str,
                   icu_name: str,
                   lookback: int=12,
                   bin_size: int=15):
    """_summary_

    Args:
        lookback (12): _description_
        bin_size (15): _description_
    """
    query = f"""
        -- 1) Positive sepsis stays (your CSV imported into BigQuery)
        WITH sepsis_pos AS (
            SELECT
            stay_id,
            -- make sure this is TIMESTAMP; cast if it's DATETIME
            TIMESTAMP(sofa_time) AS onset_time
            FROM `{sepsis3_table}`
        ),
        -- 2) Generate 15-minute bins for 12h lookback
        time_grid AS (
            SELECT
                s.stay_id,
                t AS bin_start,
                TIMESTAMP_ADD(t, INTERVAL {bin_size} MINUTE) AS bin_end,
                TIMESTAMP_DIFF(t, s.onset_time, MINUTE) AS minutes_from_onset
                FROM sepsis_pos s,
                UNNEST(GENERATE_TIMESTAMP_ARRAY(
                    TIMESTAMP_SUB(s.onset_time, INTERVAL {lookback} HOUR),
                    s.onset_time,
                    INTERVAL {bin_size} MINUTE
                )) AS t
        ),
        -- 3) Aggregate chartevents into each 15-min bin
        vitals_15min AS (
            SELECT
                g.stay_id,
                g.bin_start AS charttime,
                g.minutes_from_onset,
                -- Heart rate (example itemids from MIMIC-IV)
                AVG(CASE WHEN c.itemid IN (220045, 220046, 220047) THEN c.valuenum END) AS heart_rate_mean,
                -- MAP
                AVG(CASE WHEN c.itemid IN (220052, 220181, 225312) THEN c.valuenum END) AS map_mean,
                -- SBP
                AVG(CASE WHEN c.itemid IN (220179, 220050) THEN c.valuenum END) AS sbp_mean,
                -- DBP
                AVG(CASE WHEN c.itemid IN (220180, 220051) THEN c.valuenum END) AS dbp_mean,
                -- Respiration
                AVG(CASE WHEN c.itemid IN (220210, 224688) THEN c.valuenum END) AS resp_rate_mean,
                -- SpO2
                AVG(CASE WHEN c.itemid IN (220277, 220227) THEN c.valuenum END) AS spo2_mean,
                -- Temperature (C)
                AVG(CASE WHEN c.itemid IN (223762, 223761) THEN c.valuenum END) AS temperature_mean,
                1 AS label
            FROM time_grid g
            LEFT JOIN `{icu_name}.chartevents` c
                ON c.stay_id = g.stay_id
                AND TIMESTAMP(c.charttime) >= g.bin_start
                AND TIMESTAMP(c.charttime) <  g.bin_end
                AND c.valuenum IS NOT NULL
            GROUP BY
                g.stay_id,
                g.bin_start,
                g.minutes_from_onset
        )
        SELECT *
        FROM vitals_15min
        ORDER BY stay_id, charttime;
    """
    
    query = f"""
        -- Positive sepsis sequences from raw chartdata (chartevents),
        -- with 15-min bins and per-vital features.

        WITH sepsis_pos AS (
            -- Your Sepsis-3 positive table
            SELECT
                stay_id,
                -- Cast to DATETIME to match chartevents.charttime
                DATETIME(sofa_time) AS onset_time
            FROM `{sepsis3_table}`
        ),
        -- 1. Extract only the vital signs we care about from chartevents,
        --    and map itemid -> vital name.
        ce_vitals AS (
            SELECT
                c.stay_id,
                c.charttime,
                c.itemid,
                c.valuenum,
                CASE
                -- FILL IN THE ITEMID LISTS WITH YOURS
                WHEN c.itemid IN (220045 /*, ... */) THEN 'heart_rate'
                WHEN c.itemid IN (220052 /*, ... */) THEN 'map'
                WHEN c.itemid IN (220179 /*, ... */) THEN 'sbp'
                WHEN c.itemid IN (220180 /*, ... */) THEN 'dbp'
                WHEN c.itemid IN (220210 /*, ... */) THEN 'resp_rate'
                WHEN c.itemid IN (220277 /*, ... */) THEN 'spo2'
                WHEN c.itemid IN (223761, 223762 /*, ... */) THEN 'temperature'
                ELSE NULL
                END AS vital
            FROM `{icu_name}.chartevents` c
            WHERE c.valuenum IS NOT NULL
        ),

        ce_vitals_filt AS (
            -- Keep only rows that mapped to a vital
            SELECT *
            FROM ce_vitals
            WHERE vital IS NOT NULL
        ),

        -- 2. First available vital time per stay (for those stays that are sepsis+)
        first_data AS (
            SELECT
                v.stay_id,
                MIN(v.charttime) AS first_data_time
            FROM ce_vitals_filt v
            JOIN sepsis_pos s USING (stay_id)
            GROUP BY v.stay_id
        ),

        -- 3. Compute actual_start per stay = max(onset_time - 12h, first_data_time)
        bounds AS (
            SELECT
                s.stay_id,
                s.onset_time,
                f.first_data_time,
                DATETIME_SUB(s.onset_time, INTERVAL {lookback} HOUR) AS desired_start,
                GREATEST(
                    DATETIME_SUB(s.onset_time, INTERVAL {lookback} HOUR),
                    f.first_data_time
                ) AS actual_start
            FROM sepsis_pos s
            JOIN first_data f USING (stay_id)
        ),

        -- 4. Restrict vitals to [actual_start, onset_time] per stay
        ce_window AS (
            SELECT
                v.stay_id,
                v.charttime,
                v.vital,
                v.valuenum,
                b.onset_time,
                b.actual_start
            FROM ce_vitals_filt v
            JOIN bounds b USING (stay_id)
            WHERE
                v.charttime >= b.actual_start
                AND v.charttime <= b.onset_time
        ),

        -- 5. Bin into 15-min intervals per (stay_id, vital)
        binned AS (
            SELECT
                stay_id,
                vital,
                -- Floor charttime to previous 15-min boundary (DATETIME)
                DATETIME_SUB(
                    DATETIME_TRUNC(charttime, MINUTE),
                    INTERVAL MOD(EXTRACT(MINUTE FROM charttime), {bin_size}) MINUTE
                ) AS bin_start,
                charttime AS event_time,
                valuenum,
                onset_time
            FROM ce_window
        ),

        -- 6. Aggregate within each (stay_id, vital, bin_start)
        agg AS (
            SELECT
                stay_id,
                vital,
                bin_start,

                MIN(valuenum) AS min_val,
                MAX(valuenum) AS max_val,
                AVG(valuenum) AS mean_val,
                STDDEV(valuenum) AS std_val,
                COUNT(*) AS count_val,

                -- first / last values by time within the bin
                ARRAY_AGG(valuenum ORDER BY event_time ASC)[OFFSET(0)]  AS first_val,
                ARRAY_AGG(valuenum ORDER BY event_time DESC)[OFFSET(0)] AS last_val,

                -- first / last timestamps within the bin
                ARRAY_AGG(event_time ORDER BY event_time ASC)[OFFSET(0)]  AS first_time,
                ARRAY_AGG(event_time ORDER BY event_time DESC)[OFFSET(0)] AS last_time,

                -- onset_time is constant for stay_id, so we can use ANY_VALUE
                ANY_VALUE(onset_time) AS onset_time
            FROM binned
            GROUP BY stay_id, vital, bin_start
        ),

        -- 7. Add slope = (last - first) / seconds
        agg_slope AS (
            SELECT
                stay_id,
                vital,
                bin_start,
                min_val,
                max_val,
                mean_val,
                std_val,
                count_val,
                first_val,
                last_val,
                first_time,
                last_time,
                onset_time,
                SAFE_DIVIDE(
                    last_val - first_val,
                    DATETIME_DIFF(last_time, first_time, SECOND)
                ) AS slope
            FROM agg
        ),

        -- 8. Add prev_last_val and delta_last per (stay_id, vital)
        with_delta AS (
            SELECT
                *,
                LAG(last_val) OVER (
                    PARTITION BY stay_id, vital
                    ORDER BY bin_start
                ) AS prev_last_val
            FROM agg_slope
        )

        -- 9. Final positive sequences:
        SELECT
            stay_id,
            vital,
            bin_start,
            -- conceptual bin end (not used for filtering, just for reference)
            DATETIME_ADD(bin_start, INTERVAL 15 MINUTE) AS bin_end,

            -- time of bin (using last measurement time) relative to onset
            DATETIME_DIFF(last_time, onset_time, MINUTE) AS minutes_from_onset,

            min_val,
            max_val,
            mean_val,
            std_val,
            count_val,
            first_val,
            last_val,
            slope,
            prev_last_val,
            last_val - prev_last_val AS delta_last,
            1 AS label   -- positive sepsis
        FROM with_delta
        -- ensure bin is purely pre-onset (last measurement <= onset_time)
        WHERE last_time <= onset_time
        ORDER BY stay_id, vital, bin_start;

    """
    return query

def positive_signal_query(sepsis3_table:str,
                          icu_name: str,
                          signal_name: str,
                          arterial_items: List[str],
                          cuff_items: List[str],
                          max_value: int,
                          min_value: int=0,
                          lookback: int=12,
                          bin_size: int=15):
    """_summary_

    Args:
        sepsis3_table (str): _description_
        icu_name (str): _description_
        signal_name (str): _description_
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
        WITH vital_events AS (
            -- ---------------------------------
            -- Raw vital events for positive stays
            -- ---------------------------------
            SELECT
                SAFE_CAST(s.stay_id AS INT64) AS stay_id,
                s.sofa_time AS onset_time,
                ce.charttime,
                ce.valuenum,

                -- 15-minute bin
                DATETIME(
                    TIMESTAMP_SECONDS(
                        DIV(UNIX_SECONDS(TIMESTAMP(ce.charttime)), 900) * 900
                    )
                ) AS bin_start,

                CASE
                    WHEN ce.itemid IN ({sql_in_list(arterial_items)}) THEN 'arterial'
                    WHEN ce.itemid IN ({sql_in_list(cuff_items)}) THEN 'cuff'
                END AS modality
            FROM `{sepsis3_table}` s
            JOIN `{icu_name}.chartevents` ce
                ON s.stay_id = ce.stay_id
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
            WHERE e.charttime BETWEEN
                    GREATEST(
                        DATETIME_SUB(DATETIME(e.onset_time), INTERVAL 12 HOUR),
                        f.first_data_time
                    )
                    AND DATETIME(e.onset_time)
        ),
        vital_modality_bins AS (
            -- ---------------------------------
            -- Aggregate per bin & modality
            -- ---------------------------------
            SELECT
                stay_id,
                bin_start,
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
            GROUP BY stay_id, bin_start, modality
        ),
        vital_fused_bins AS (
            -- ---------------------------------
            -- Option A: rank-and-pick fusion
            -- ---------------------------------
            SELECT
                stay_id,
                bin_start,

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
            GROUP BY stay_id, bin_start
        ),
        final AS (
            -- ---------------------------------
            -- Derived features + labels
            -- ---------------------------------
            SELECT
                stay_id,
                bin_start,

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

                TIMESTAMP_DIFF(TIMESTAMP(onset_time), TIMESTAMP(vital.last_time), MINUTE) AS minutes_from_onset,

                CASE vital.modality
                    WHEN 'arterial' THEN 1
                    WHEN 'cuff'     THEN 2
                END AS vital_source_type,

                LAG(vital.last_val) OVER (PARTITION BY stay_id ORDER BY bin_start) AS prev_last_val,
                vital.last_val - LAG(vital.last_val) OVER (PARTITION BY stay_id ORDER BY bin_start) AS delta_last,
                1 AS label
            FROM vital_fused_bins
            JOIN (
                SELECT DISTINCT stay_id, onset_time
                FROM restricted
            ) USING (stay_id)
        )
        SELECT *
        FROM final
        WHERE minutes_from_onset >= 0   -- no leakage safety
        ORDER BY stay_id, bin_start;
    """
    
    return query

