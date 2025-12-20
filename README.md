# 🚑 Early Sepsis Detection and Clinical Action Forecasting with Multimodal Transformers

## 📌 Project Overview
Sepsis is a life-threatening medical condition responsible for millions of deaths worldwide each year. Early identification and timely clinical intervention—particularly blood culture collection and systemic antibiotic administration—are critical to improving patient outcomes. However, sepsis often develops subtly, making early detection challenging even for experienced clinicians.

This project presents a **multimodal deep learning system** that continuously analyzes ICU patient data to:
1. **Detect early sepsis risk before clinical actions occur**
2. **Forecast the need for key clinical interventions**, such as blood culture orders and systemic antibiotics
3. **Update sepsis risk dynamically** once those interventions are initiated
4. **Provide interpretable explanations** to support clinician decision-making

The model is trained and evaluated using the **MIMIC-IV v3.1** critical care dataset.

---

## 🧠 Core Idea
Instead of treating sepsis as a static classification problem, this project models sepsis as a **temporal clinical decision process**:

- **Stage 1 (Early Triage)**  
  Predict the likelihood that a patient will soon require:
  - A blood culture (BC)
  - Systemic antibiotic therapy  

  These actions together serve as a clinically grounded proxy for high suspicion of sepsis.

- **Stage 2 (Sequential Update)**  
  Once a BC is ordered and antibiotics are administered, the model updates the long-term sepsis trajectory and risk estimate in real time.

This design aligns closely with real ICU workflows and sepsis-3 clinical guidelines.

---

## 🧩 Data Sources
- **MIMIC-IV ICU & Hospital Modules**
- **MIMIC-IV Derived Schema (`mimiciv_derived`)**
- **Sepsis-3 cohort** constructed using the official MIMIC sepsis-3 SQL logic

### Modalities Used
- **Structured time-series data**
  - Vital signs (e.g., HR, SBP, DBP, MAP, RR, SpO₂)
  - Aggregated every **15 minutes** (min / max / mean / std / slope / deltas)
- **Clinical interventions**
  - Blood culture orders
  - Systemic antibiotic initiation
- **Unstructured clinical text**
  - Radiology reports
  - Discharge summaries
- **Derived clinical scores**
  - SOFA score (used only for labeling, not as model input)

---

## 🏗 Model Architecture
The system uses a **Transformer-based temporal fusion architecture** with the following components:

- **Time-series encoder**
  - Transformer encoder with masking for irregular sampling
  - Handles missing data explicitly
- **Text encoder**
  - Pretrained clinical language model (e.g., ClinicalBERT)
  - Attention pooling over notes within each time window
- **Multimodal fusion**
  - Cross-attention between vitals and note embeddings
- **Multi-horizon prediction heads**
  - Predict sepsis risk at multiple future horizons (e.g., 1h, 2h, 4h)
  - Predict likelihood of BC order and antibiotic initiation
- **Interpretability hooks**
  - Temporal attention weights
  - Feature attribution over vitals and note segments

---

## 🔍 Explainability & Clinical Insight
When the model outputs a high sepsis risk score, it also provides:
- Key contributing vital sign trends (e.g., falling MAP, rising RR)
- Time windows with highest attention weights
- Relevant phrases or sections from clinical notes

This allows clinicians to **understand why the model is concerned**, increasing trust and usability in real clinical settings.

---

## 🧪 Training Strategy
- **Positive samples**
  - Time windows preceding sepsis-3 onset
- **Negative samples**
  - True negative ICU stays
  - Early non-septic windows from septic patients
- **Handling variable sequence length**
  - Masked attention
  - Flexible lookback horizons (up to 12 hours)
- **Loss function**
  - Multi-task loss combining:
    - Sepsis risk
    - Blood culture prediction
    - Antibiotic initiation prediction

---

## 📊 Evaluation Metrics
- AUROC / AUPRC for sepsis prediction
- Lead time (hours before clinical intervention)
- Calibration curves
- Ablation studies on modalities
- Qualitative explanation examples

---

## 📁 Repository Structure
