import os
from typing import List
import pandas as pd
import numpy as np
import torch
from torch import nn

import lightning.pytorch as pl
from pytorch_forecasting import TimeSeriesDataSet, NaNLabelEncoder
from pytorch_forecasting import TemporalFusionTransformer, Baseline
from pytorch_lightning.callbacks import ModelCheckpoint


class WeightedBCE(nn.Module):
    def __init__(self, pos_weight):
        super().__init__()
        # pos_weight: tensor of shape [n_horizons]
        self.pos_weight = pos_weight

    def forward(self, y_pred, y_true):        
        n_horizons = len(self.pos_weight)
        # BCEWithLogitsLoss with reduction='none'
        loss = nn.BCEWithLogitsLoss(reduction="none")
        l = loss(y_pred, y_true)  # shape: [batch_size * n_horizons]
        
        # Repeat pos_weight to match batch
        batch_size = y_pred.shape[0] // n_horizons
        # pos_weight: [1, n_horizons] -> repeat for batch -> flatten
        weight = self.pos_weight.unsqueeze(0).repeat(batch_size, 1).flatten()
        l = l * weight
        return l.mean()

class Sepsis3Model():
    def __init__(self, data_path:str):
        self.data_path = data_path
        self.vital_sign = ['heart_rate', "dbp", "mbp", "sbp", "resp", "spo2", "temp"]
        
        fields = ["mean_val", "std_val", "last_val", "delta_last", "count_val", "measured_flag"]
        self.dynamic_real_cols = [f"{name}_{field}" for name in self.vital_sign for field in fields]
        self.dynamic_cat_cols = [f"{name}_source_type" for name in self.vital_sign]
        self.static_real_cols = ["age"]
        self.static_cat_cols = ["gender", "icu_type", "admission_type"]
        
        self.batch_size = 256
        self.min_encoder_length = 1
        self.max_encoder_length = 12 #6 hours
        self.pred_horizons = [2, 4, 6, 8, 10, 12]
        self.max_prediction_length = self.pred_horizons[-1]
        self.target_cols = [f"target_{m}" for m in self.pred_horizons]
        
    def create_horizon_targets(self, df:pd.DataFrame, horizon_bins:List[int]):
        """_summary_

        Args:
            df (pd.DataFrame): _description_
            horizons_bins (List[int]): _description_
        """
        def process_stay(group):
            group = group.sort_values("bin_index").copy()
            T = len(group)

            if group["label"].iloc[0]==1:  # positive stay
                for h in horizon_bins:
                    target = np.zeros(T, dtype=np.float32)
                    # first h bins get target=1
                    start_idx = max(0, T - h)
                    target[start_idx:] = 1
                    group[f"target_{h}"] = target
            else:  # negative stay
                for h in horizon_bins:
                    group[f"target_{h}"] = 0.0

            return group

        return df.groupby("stay_id", group_keys=False).apply(process_stay)
    
    def training(self):
        print("Loading the data....")
        df_train = pd.read_parquet(os.path.join(self.data_path, "sepsis3_training_2026-02-21.parquet"))
        df_validation = pd.read_parquet(os.path.join(self.data_path, "sepsis3_validation_2026-02-21.parquet"))
        df_train = self.create_horizon_targets(df_train, self.pred_horizons)
        df_validation = self.create_horizon_targets(df_validation, self.pred_horizons)
        
        #filter out very short stay and very long stay (30 days)
        max_seq_len = 30*24*2
        min_seq_len = self.min_encoder_length + self.max_prediction_length
        df_train = df_train.groupby("stay_id").filter(lambda g: len(g) >= min_seq_len and len(g)<=max_seq_len)
        df_validation = df_validation.groupby("stay_id").filter(lambda g: len(g) >= min_seq_len and len(g)<=max_seq_len)
                
        cols = [name for name in self.dynamic_real_cols if "flag" not in name]
        for col in cols:  # all dynamic real features
            df_train[col] = df_train[col].fillna(0.0)
            df_validation[col] = df_validation[col].fillna(0.0)
        
        for col in self.static_cat_cols + self.dynamic_cat_cols:
            df_train[col] = df_train[col].fillna(-1).astype(int).astype(str)
            df_validation[col] = df_validation[col].fillna(-1).astype(int).astype(str)
        
        print("start the training....")
        
        pos_weight_list = []
        for col in self.target_cols:
            pos = df_train[col].sum()
            neg = len(df_train[col]) - pos
            pos_weight = neg / pos
            pos_weight_list.append(pos_weight)
        
        training = TimeSeriesDataSet(
            df_train,
            time_idx="bin_index",
            target=self.target_cols,
            group_ids=["stay_id"],  # each ICU stay
            min_encoder_length=self.min_encoder_length,
            max_encoder_length=self.max_encoder_length,
            min_prediction_length=self.max_prediction_length,
            max_prediction_length=self.max_prediction_length,
            static_categoricals=self.static_cat_cols,
            static_reals=self.static_real_cols,
            time_varying_known_categoricals=None,
            time_varying_known_reals=["bin_index"],
            time_varying_unknown_categoricals=self.dynamic_cat_cols,
            time_varying_unknown_reals=self.dynamic_real_cols,
            add_relative_time_idx=True,
            add_target_scales=False,
            add_encoder_length=True,
            categorical_encoders={
                # allow unseen categories to be treated as NaN
                col: NaNLabelEncoder(add_nan=True) for col in self.static_cat_cols + ["stay_id"]
            }
        )
        
        validation = TimeSeriesDataSet.from_dataset(training, df_validation, predict=True, stop_randomization=True)
        train_loader = training.to_dataloader(train=True, batch_size=self.batch_size, num_workers=0)
        val_loader   = validation.to_dataloader(train=False, batch_size=self.batch_size, num_workers=0)
        
        pos_weight_tensor = torch.tensor(pos_weight_list, dtype=torch.float32)
        loss = WeightedBCE(pos_weight_tensor)
        
        tft = TemporalFusionTransformer.from_dataset(
            training,
            learning_rate=1e-3,
            hidden_size=64,
            attention_head_size=4,
            dropout=0.1,
            hidden_continuous_size=32,
            output_size=[1 for _ in range(len(self.target_cols))],
            loss=loss,
            log_interval=10,
            reduce_on_plateau_patience=4
        )
        
        checkpoint_callback = ModelCheckpoint(
            dirpath="checkpoints/",       # folder to save
            filename="tft-sepsis3-{epoch}-{val_loss:.4f}",  # filename template
            save_top_k=15,                 # only keep best model
            verbose=True,
            monitor="val_loss",           # metric to monitor
            mode="min"                    # minimize val_loss
        )
        
        trainer = pl.Trainer(
            max_epochs=30,
            accelerator="cpu",          # force CPU
            devices=1,                  # number of CPUs
            gradient_clip_val=0.1,
            limit_train_batches=1.0,    # use 100% of batches
            limit_val_batches=1.0,
            callbacks=[checkpoint_callback],
            enable_checkpointing=True, # optional for CPU run
            log_every_n_steps=10
        )
        
        trainer.fit(
            tft,
            train_dataloaders=train_loader,
            val_dataloaders=val_loader
        )
        
        #baseline_predictions = Baseline().predict(val_loader)
    
    
if __name__ == "__main__":
    data_path = "sepsis_data"
    trainer = Sepsis3Model(data_path)
    trainer.training()

