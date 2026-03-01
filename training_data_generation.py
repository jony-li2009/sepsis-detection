# Step 1. Anchor sepsis onset time

# Suppose you already have a table called sepsis_events with:

# subject_id,
# stay_id,
# sepsis_onset_time


# This is the anchor for positive samples.

# Step 2. Extract ICU time series features

# Example for vital signs (chartevents):

# CREATE OR REPLACE TABLE myproject.mimic_derived.vitals_24h_presepsis AS
# SELECT
#   s.subject_id,
#   s.stay_id,
#   c.charttime,
#   DATETIME_DIFF(s.sepsis_onset_time, c.charttime, HOUR) AS hours_before_onset,
#   c.itemid,
#   c.valuenum
# FROM `physionet-data.mimiciv_icu.chartevents` c
# JOIN `myproject.mimic_derived.sepsis_events` s
#   ON c.stay_id = s.stay_id
# WHERE c.charttime BETWEEN TIMESTAMP_SUB(s.sepsis_onset_time, INTERVAL 24 HOUR) 
#                       AND s.sepsis_onset_time
#   AND c.valuenum IS NOT NULL
#   AND c.itemid IN (
#       -- MAP, HR, Temp, etc. (you’ll need to map these itemids from d_items)
#   )


# Do the same for:

# Labs (labevents from hosp)

# Medications (inputevents for vasopressors, prescriptions for antibiotics)

# Outputs (outputevents for urine)

# Step 3. Create Negative Cohort

# Pick “pseudo-onset times” for patients without sepsis.
# Example: choose a random time 24h after ICU admission.

# CREATE OR REPLACE TABLE myproject.mimic_derived.control_events AS
# SELECT
#   ie.subject_id,
#   ie.stay_id,
#   TIMESTAMP_ADD(ie.intime, INTERVAL 24 HOUR) AS pseudo_onset_time
# FROM `physionet-data.mimiciv_icu.icustays` ie
# WHERE NOT EXISTS (
#   SELECT 1 FROM myproject.mimic_derived.sepsis_events s
#   WHERE s.stay_id = ie.stay_id
# );


# Then reuse the same query logic as above, just swap in pseudo_onset_time.

# Step 4. Merge into patient-time matrix

# Now you want one regular time grid (e.g., hourly).

# In BigQuery you can generate it like:

# WITH time_grid AS (
#   SELECT
#     s.subject_id,
#     s.stay_id,
#     TIMESTAMP_ADD(s.sepsis_onset_time, INTERVAL -hour HOUR) AS grid_time
#   FROM myproject.mimic_derived.sepsis_events s,
#        UNNEST(GENERATE_ARRAY(1,24)) AS hour
# )

# SELECT
#   g.subject_id,
#   g.stay_id,
#   g.grid_time,
#   MAX(CASE WHEN c.itemid = 220045 THEN c.valuenum END) AS mean_arterial_pressure,
#   MAX(CASE WHEN c.itemid = 220210 THEN c.valuenum END) AS temperature,
#   MAX(CASE WHEN c.itemid = 220277 THEN c.valuenum END) AS heartrate,
#   ...
# FROM time_grid g
# LEFT JOIN myproject.mimic_derived.vitals_24h_presepsis c
#   ON g.stay_id = c.stay_id
#  AND TIMESTAMP_TRUNC(c.charttime, HOUR) = TIMESTAMP_TRUNC(g.grid_time, HOUR)
# GROUP BY g.subject_id, g.stay_id, g.grid_time


# This creates a patient × time × variable matrix.
# Missing values will show up as NULL — which you can later forward-fill or impute.

# Step 5. Export to CSV for ML

# From BigQuery → Python (pandas) → train your ML model.

# from google.cloud import bigquery
# import pandas as pd

# client = bigquery.Client()
# query = "SELECT * FROM myproject.mimic_derived.patient_time_matrix"
# df = client.query(query).to_dataframe()

# # Example: forward fill per patient
# df = df.sort_values(["subject_id", "stay_id", "grid_time"])
# df = df.groupby(["subject_id", "stay_id"]).ffill()


# ✅ At this point you’ll have:

# Positive cohort (true sepsis, anchored at onset)

# Negative cohort (pseudo-onset, no sepsis)

# Hourly (or 30-min) sampled time series window (24h, 12h, etc.)

# This is exactly the dataset you need to feed into your time series ML models (ARLSTM, TFT, transformers).