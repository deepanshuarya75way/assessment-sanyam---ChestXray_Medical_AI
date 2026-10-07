from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
import psycopg2
import psycopg2.extras
import csv
import io
import os
from datetime import datetime


app = FastAPI(
  title="PREDICTION FEEDBACK AND TRAINING DATASET API",
  version="1.0.0"
)

DB_CONFIG = {
  "host": os.getenv("DB_HOST","localhost"),
  "database": os.getenv("DB_NAME", "prediction_feedback"),
  "user": os.getenv("DB_USER","postgres"),
  "password": os.getenv("DB_PASSWORD", ""),
  "port": int(os.getenv("DB_PORT","5432"))
}

def get_connection():
  return psycopg2.connect(
    host=os.getenv("DB_HOST","localhost"),
    database=os.getenv("DB_NAME","prediction_feedback"),
    user=os.getenv("DB_USER","postgres"),
    password=os.getenv("DB_PASSWORD", "feedback123"),
    port=int(os.getenv("DB_PORT","5432"))
  )

def initialize_database():
  conn = get_connection()
  cur = conn.cursor()
  cur.execute("""CREATE TABLE
  prediction_feedback (
  id SERIAL PRIMARY KEY, prediction_id VARCHAR(100) UNIQUE NOT NULL,
  patient_id VARCHAR(100) NOT NULL,
  patient_matadeta JSONB NOT NULL,
  original_prediction varchar(100) NOT NULL,
  corrected_label VARCHAR(100) NOT NULL,
  feedback_text TEXT,
  eligible_for_training BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  
  )
  """)
  conn.commit()
  cur.close()
  conn.close()

@app.on_event("startup")
def startup():
  initialize_database()

class FeedbackRequest(BaseModel):
  prediction_id: str
  patient_id: str
  patient_matadeta: Dict[str, Any]
  original_prediction: str
  corrected_label:str
  feedback_text: Optional[str] = None

ALLOWED_LABELS = {
  "NORMAL", "PNEUMONIA", "COVID", "TUBERCULOSIS", "OTHER"
}

def validate_feedback(data):
  if not data.patient_metadeta:
    return False, "Patient metadata is required."
  if not data.original_prediction.strip():
    return False, "Originaal predictiion is reqd."
  label = data.corrected_label.strip().upper()
  if label not in ALLOWED_LABELS:
    return False, "Invalid corrected_label."

  return True, label

@app.get("/")
def root():
  return {"status": "running"}

@app.get("/health")
def health():
  try:
    conn = get_connection()
    conn.close()
    return {"status": "healthy","database": "connected"}

  except Exception as e:
    raise HTTPException(500,str(e))

@app.post("/feedback")
def submit_feedback(data: FeedbackRequest):
  valid, result = validate_feedback(data)
  if not valid:
    raise HTTPException(400, result)

  conn = get_connection()
  cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
  cur.execute("SELECT id FROM prediction_feedback WHERE prediction_id=%s",(data.prediction_id,))
  if cur.fetchone():
    cur.close()
    conn.close()
    raise HTTPExceptiom(409,"Feedback already exists.")

  cur.execute("""
  INSERT INTO prediction_feedback(prediction_id, patient_id, patient_metadata, original_prediction, corrected_label, feedback_text, eligible_for_training)
  VALUES (%s,%s,%s,%s,%s,%s,TRUE)
  RETURNING id, created_at  
  """, (
    data.prediction_id,
    data.patient_id,
    psycopg2.extras.Json(data.patient_metadata),
    data.original_prediction.strip(),
    result,
    data.feedback_text
    ))

  row = cur.fetchone()
  conn.commit()
  cur.close()
  conn.close()

  return {
    "message": "Feedback submitted successfully",
    "feedback_id": row["id"],
    "eligible_for_training": True
  }


@app.get("/feedback")
def get_feedback():
  conn = get_connection()
  cur = conn.cursor(
    cursor_factory = psycopg2.extras.RealDictCursor
  )
  cur.execute(
    "SELECT * FROM prediction_feedback ORDER BY created_at DESC"
  )
  rows = cur.fetchall()
  cur.close()
  conn.close()

  return {"count": len(rows),"feedback": rows}

@app.get("/training-dataset")
def training_dataset():
  conn = get_connection()
  cur = conn.cursor(
    cursor_factory=psycopg2.extras.RealDictCursor
  )
  cur.execute("""
  SELECT prediction_id, patient_id, patient_metadata, original_prediction, corrected_label, feedback_text, created_at FROM prediction_feedback WHERE eligible_for_training=TRUE ORDER by created_at
  """)
  rows = cur.fetchall()
  cur.close()
  conn.close()

  return{
    "dataset_size": len(rows),
    "dataset": rows
  }

@app.get("/training-dataset/export")
def export_dataset():
  conn = get_connection()
  cur = conn.cursor()
  cur.execute("""
  SELECT prediction_id, patient_id, patient_metadata, original_prediction, corrected_label, feedback_text, created_at FROM prediction_feedback WHERE eligible_for_training=TRUE ORDER BY created_at  
  """)

  rows = cur.fetchall()
  cur.close()
  conn.close()

  output = io.StringIO()
  writer = csv.writer(output)

  writer.writerow([
    "prediction_id", "patient_id", "patient_metadata", "original_prediction", "corrected_label", "feedback_text", "created_at"
  ])
  writer.writerows(rows)
  output.seek(0)
  
  return StreamingResponse(
    iter([output.getvalue()]),
    media_type="text/csv",
    headers={
      "Content-Disposition":"attachment; filename=training_dataset.csv"
    }
  )