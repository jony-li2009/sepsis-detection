import os
from tqdm import tqdm
import pandas as pd
from google.cloud import bigquery

from sepsis3_query import suspicion_infection, SOFA
from sepsis3_positive_query import positive_signal_query, positive_temp_query
from sepsis3_negative_query import negative_signal_query, negative_temp_query

class SepsisDataExtractor():
    def __init__(self,
                 data_path:str,
                 credentials_path: str,
                 project_id:str="mimic4-sepsis-468620"):
        """_summary_

        Args:
            data_path (str): _description_
            credentials_path (str): _description_
            project_id (str, optional): _description_. Defaults to "mimic4-sepsis-468620".
        """
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        self.gbq_client = bigquery.Client(project=project_id)
        
        self.data_path = data_path
        self.hosp_name = "physionet-data.mimiciv_3_1_hosp"
        self.icu_name = "physionet-data.mimiciv_3_1_icu"
        self.note_name = "physionet-data.mimiciv_note"
        self.derived_name = "physionet-data.mimiciv_3_1_derived"
        self.sepsis3_table = 'mimic4-sepsis-468620.sepsis3.sepsis3_pos'
                      
        #sbp: 220179, 220050, 225309 Systolic BP
        #dbp: 220180, 220051, 225310 Diastolic BP
        #mbp: 220052, 220181, 225312 Mean BP
        #resp_rate:  220210 - common nurse-charted/bedside 224690 - total/ventilator/monitor. higher-frequency perfer 224690 if is available
        #glucose: 225664, 220621, 226537
        
        self.sepsis3_specimens = [
            "BLOOD CULTURE",
            "BLOOD",
            "BLOOD CULTURE ( MYCO/F LYTIC BOTTLE)",
            "FLUID RECEIVED IN BLOOD CULTURE BOTTLES"
        ]        
                
    def suspicion_of_infection(self):
        """_summary_
        """
        query = suspicion_infection(self.hosp_name, self.derived_name)
        df = self.gbq_client.query(query).to_dataframe()
        fname = os.path.join(self.data_path, "suspicion_of_infection.csv")
        df.to_csv(fname, index=False)
        
    def SOFA_data(self):
        """_summary_
        """
        query = SOFA(self.derived_name)
        df = self.gbq_client.query(query).to_dataframe()
        fname = os.path.join(self.data_path, "SOFA.csv")
        df.to_csv(fname, index=False)        
        
    def sepsis_data3(self):
        """_summary_
        """
        fname = os.path.join(self.data_path, "suspicion_of_infection.csv")
        df_SoI = pd.read_csv(fname, low_memory=False)
        time_cols_soi = ["antibiotic_time", "culture_time", "suspected_infection_time"]
        for col in time_cols_soi:
            df_SoI[col] = pd.to_datetime(df_SoI[col])
        
        fname = os.path.join(self.data_path, "SOFA.csv")
        df_SOFA = pd.read_csv(fname)
        time_cols_sofa = ["starttime", "endtime"]
        for col in time_cols_sofa:
            df_SOFA[col] = pd.to_datetime(df_SOFA[col])
            
        # Inner join on stay_id
        df_s1 = df_SoI.merge(df_SOFA, on="stay_id", how="inner", suffixes=("", "_sofa"))
        # Time window filter
        mask_time = (
            df_s1["endtime"] >= df_s1["suspected_infection_time"] - pd.Timedelta(hours=48)
        ) & (
            df_s1["endtime"] <= df_s1["suspected_infection_time"] + pd.Timedelta(hours=24)
        )
        df_s1 = df_s1[mask_time].copy()
        # Only include in-ICU rows (same as WHERE soi.stay_id IS NOT NULL)
        df_s1 = df_s1[df_s1["stay_id"].notna()].copy()
        
        # sepsis3 flag
        df_s1["sepsis3"] = (df_s1["sofa_score"] >= 2) & (df_s1["suspected_infection"] == 1)
        # Sort exactly as in SQL
        df_s1 = df_s1.sort_values(
            ["stay_id", "suspected_infection_time", "antibiotic_time", "culture_time", "endtime"]
        )
        # Row number per stay_id (1-based)
        df_s1["rn_sus"] = df_s1.groupby("stay_id").cumcount() + 1
        
        df_final = df_s1[df_s1["rn_sus"] == 1].copy()

        df_final = df_final[[
            "subject_id", "stay_id", 'antibiotic', 'route',
            "antibiotic_time", "culture_time", "suspected_infection_time", 'specimen',
            "endtime",          # will rename to sofa_time
            "sofa_score",
            "respiration", "coagulation", "liver",
            "cardiovascular", "cns", "renal",
            "sepsis3"
        ]].rename(columns={
            "endtime": "sofa_time"
        })
        
        fname = os.path.join(self.data_path, "sepsis3.csv")
        df_final.to_csv(fname, index=False)
        
    def sepsis3_positve_data(self):
        signal_data = [
            {
                "name": "heart_rate",
                "arterial": [220045],
                "cuff": [],
                "min": 0,
                "max": 300
            },            
            {
                "name": "sbp",
                "arterial": [220050, 225309],
                "cuff": [220179],
                "min": 0,
                "max": 400
            },
            {
                "name": "dbp",
                "arterial": [220051, 225310],
                "cuff": [220180],
                "min": 0,
                "max": 300
            },
            {
                "name": "mbp",
                "arterial": [220052, 225312],
                "cuff": [220181],
                "min": 0,
                "max": 300
            },
            {
                "name": "resp",
                "arterial": [224690],
                "cuff": [220210],
                "min": 0,
                "max": 70
            },
            {
                "name": "spo2",
                "arterial": [220277],
                "cuff": [],
                "min": 0,
                "max": 100
            },
        ]
        
        #positives
        # for signal in signal_data:
        #     print(signal['name'])
        #     query = positive_signal_query(self.sepsis3_table, self.sepsis3_specimens, self.icu_name,
        #                               signal['arterial'], signal['cuff'], signal['max'])
            
        #     df = self.gbq_client.query(query).to_dataframe()
        #     fname = os.path.join(self.data_path, f"sepsis3_positive_{signal['name']}_seq.csv")
        #     df.to_csv(fname, index=False)
        
        # #temperature
        # query = positive_temp_query(self.sepsis3_table, self.sepsis3_specimens, self.icu_name)        
        # df = self.gbq_client.query(query).to_dataframe()
        # fname = os.path.join(self.data_path, f"sepsis3_positive_temp_seq.csv")
        # df.to_csv(fname, index=False)
            
        for signal in signal_data:
            print(signal['name'])
            query = negative_signal_query(self.sepsis3_table, self.sepsis3_specimens, self.icu_name,
                                      signal['arterial'], signal['cuff'], signal['max'])
            
            df = self.gbq_client.query(query).to_dataframe()
            fname = os.path.join(self.data_path, f"sepsis3_negative_{signal['name']}_seq.csv")
            df.to_csv(fname, index=False)
        
        query = negative_temp_query(self.sepsis3_table, self.sepsis3_specimens, self.icu_name)        
        df = self.gbq_client.query(query).to_dataframe()
        fname = os.path.join(self.data_path, f"sepsis3_negative_temp_seq.csv")
        df.to_csv(fname, index=False)

"""

Column	        Meaning
min_val	        lowest value in bin
max_val	        highest value in bin
mean_val	    average
std_val	        variability
first_val	    earliest measurement
last_val	    latest measurement
slope	        trend in that bin
prev_last_val	previous bin's latest value
delta_last	    last_val - prev_last_val
"""

if __name__ == "__main__":
    path = "sepsis_data"
    credentials_path = "gbq_credentials.json"
    if not os.path.exists(path):
        os.makedirs(path)
    app = SepsisDataExtractor(path, credentials_path)
    #app.suspicion_of_infection()
    #app.SOFA_data()
    #app.sepsis_data3()
    app.sepsis3_positve_data()
    
