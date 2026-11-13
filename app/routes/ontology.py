import uuid
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.models.schema_models import OntologyVersionModel, DatabaseConnectionModel
from app.services.observability import ingest_trace
from app.ontology_schemas import (
    OntologySyncResponse,
    OntologySyncStatusResponse,
    OntologyCategoryResponse,
    OntologyTableListResponse,
    OntologyTableSummary,
    OntologyColumnListResponse,
    OntologyColumnSummary,
    GenerateDescriptionResponse,
    TableOntologyEdit,
    ColumnOntologyEdit,
    BusinessMetric,
    BusinessMetricsResponse
)
from app.utils.constants import LLM_ONTOLOGY_URL
from sqlalchemy import create_engine, inspect
from app.utils.crypt import decrypt_string
from app.services.query_executor import is_connector_row, saved_physical_schema

router = APIRouter()
logger = logging.getLogger(__name__)

_SYNC_JOBS: dict[str, dict] = {}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_latest_ontology(db: Session, datasource_id: uuid.UUID) -> OntologyVersionModel:
    ontology = db.query(OntologyVersionModel).filter(
        OntologyVersionModel.datasource_connection_id == datasource_id
    ).order_by(OntologyVersionModel.version_number.desc()).first()
    
    if not ontology:
        raise HTTPException(status_code=404, detail="Ontology not found. Please sync first.")
    
    return ontology


def _get_latest_ontology_optional(db: Session, datasource_id: uuid.UUID) -> Optional[OntologyVersionModel]:
    return (
        db.query(OntologyVersionModel)
        .filter(OntologyVersionModel.datasource_connection_id == datasource_id)
        .order_by(OntologyVersionModel.version_number.desc())
        .first()
    )

def _fetch_physical_schema(db: Session, datasource_id: uuid.UUID) -> dict:
    conn = db.query(DatabaseConnectionModel).filter(DatabaseConnectionModel.id == datasource_id).first()
    if not conn:
        raise HTTPException(status_code=404, detail="Database connection not found.")

    if is_connector_row(conn):
        return saved_physical_schema(conn)
    
    try:
        decrypted_url = decrypt_string(conn.db_connection_string)
        engine = create_engine(decrypted_url)
        inspector = inspect(engine)
        
        schema = {}
        for table_name in inspector.get_table_names():
            schema[table_name] = inspector.get_columns(table_name)
            
        engine.dispose()
        return schema
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to extract physical schema: {str(e)}")

def _add_pending_tables(chunk, enriched_tables):
    for t in chunk:
        enriched_tables.append({
            "physical_name": t["name"],
            "category": "Unknown",
            "description": "",
            "status": "PENDING",
            "columns": [
                {
                    "physical_name": c["name"], 
                    "business_definition": "", 
                    "semantic_type": "Unknown", 
                    "status": "PENDING"
                } for c in t["columns"]
            ]
        })

def _sync_ontology_background(datasource_id: uuid.UUID):
    key = str(datasource_id)
    db = next(get_db())
    try:
        # Extract schema
        schema = _fetch_physical_schema(db, datasource_id)
        
        # Build payload for LLM
        tables_payload = []
        for table_name, columns in schema.items():
            cols = []
            for col in columns:
                cols.append({"name": col.get("name"), "type": str(col.get("type"))})
            tables_payload.append({"name": table_name, "columns": cols})

        total = len(tables_payload)
        if key in _SYNC_JOBS:
            _SYNC_JOBS[key].update({"total_tables": total, "completed_tables": 0})

        # Per-table enrich-table (same rich fields as cenomi: purpose, concepts, questions)
        enrich_url = os.getenv(
            "ONTOLOGY_ENRICH_TABLE_URL",
            "http://127.0.0.1:8001/api/ontology/enrich-table",
        )
        conn = db.query(DatabaseConnectionModel).filter(
            DatabaseConnectionModel.id == datasource_id
        ).first()
        db_type = getattr(conn, "db_type", None) if conn else None

        enriched_tables = []
        for i, table in enumerate(tables_payload):
            start_time = time.time()
            prompt_payload = {
                "db_type": db_type or "unknown",
                "name": table.get("name"),
                "columns": table.get("columns", []),
                "relationships": [],
            }
            try:
                response = requests.post(
                    enrich_url,
                    json=prompt_payload,
                    timeout=120,
                )
                latency_ms = int((time.time() - start_time) * 1000)
                trace_payload = {
                    "project_id": str(conn.project_id) if conn and conn.project_id else None,
                    "ai_service": "ontology_sync_table",
                    "latency_ms": latency_ms,
                    "prompt_text": json.dumps(prompt_payload),
                    "status": "success",
                }
                
                if response.status_code == 200:
                    enriched = response.json()
                    
                    trace_payload["completion_text"] = response.text
                    trace_payload["prompt_tokens"] = enriched.get("prompt_tokens", 0) if isinstance(enriched, dict) else 0
                    trace_payload["completion_tokens"] = enriched.get("completion_tokens", 0) if isinstance(enriched, dict) else 0
                    trace_payload["model_name"] = enriched.get("model_name", "unknown") if isinstance(enriched, dict) else "unknown"
                    ingest_trace(db, trace_payload)
                    
                    if isinstance(enriched, dict):
                        enriched.setdefault("is_ai_generated", True)
                        # Keep drafts reviewable unless LLM already set a status
                        enriched.setdefault("status", "PENDING")
                        enriched["last_updated"] = _now_iso()
                        # Normalize confidence field for explorer UI
                        if enriched.get("confidence") is None and enriched.get("ai_confidence") is not None:
                            enriched["confidence"] = enriched.get("ai_confidence")
                        enriched_tables.append(enriched)
                    else:
                        _add_pending_tables([table], enriched_tables)
                else:
                    trace_payload["status"] = "error"
                    trace_payload["error_message"] = response.text
                    ingest_trace(db, trace_payload)
                    
                    logger.warning(
                        "enrich-table failed for %s: %s",
                        table.get("name"),
                        response.text,
                    )
                    _add_pending_tables([table], enriched_tables)
            except Exception as e:
                latency_ms = int((time.time() - start_time) * 1000)
                trace_payload = {
                    "project_id": str(conn.project_id) if conn and conn.project_id else None,
                    "ai_service": "ontology_sync_table",
                    "latency_ms": latency_ms,
                    "prompt_text": json.dumps(prompt_payload),
                    "status": "error",
                    "error_message": str(e),
                }
                ingest_trace(db, trace_payload)
                
                logger.warning("enrich-table exception for %s: %s", table.get("name"), e)
                _add_pending_tables([table], enriched_tables)

            if key in _SYNC_JOBS:
                _SYNC_JOBS[key]["completed_tables"] = min(len(enriched_tables), total)
                
        # Determine next version and preserve existing graph schema if any
        latest = _get_latest_ontology_optional(db, datasource_id)
        
        next_version = 1 if not latest else latest.version_number + 1
        
        existing_data = {}
        if latest and latest.ontology_json:
            try:
                existing_data = json.loads(latest.ontology_json)
            except Exception:
                pass
                
        existing_data["tables"] = enriched_tables
        if "business_metrics" not in existing_data:
            existing_data["business_metrics"] = []
        existing_data.setdefault("metadata", {})["catalog_generated_at"] = _now_iso()
        
        # Insert into DB
        new_ontology = OntologyVersionModel(
            datasource_connection_id=datasource_id,
            version_number=next_version,
            version_label=f"Auto-generated v{next_version}",
            status="draft",
            ontology_json=json.dumps(existing_data),
            graph_json=latest.graph_json if latest else json.dumps({"nodes": [], "edges": [], "stats": {}}),
        )
        db.add(new_ontology)
        db.commit()

        if key in _SYNC_JOBS:
            _SYNC_JOBS[key].update({
                "status": "completed",
                "completed_at": _now_iso(),
                "completed_tables": total,
            })
    except Exception as e:
        logger.exception("Background Sync Error for datasource %s", datasource_id)
        if key in _SYNC_JOBS:
            _SYNC_JOBS[key].update({
                "status": "error",
                "error": str(e),
                "completed_at": _now_iso(),
            })
    finally:
        db.close()


@router.get("/datasources/{datasource_id}/sync/status", response_model=OntologySyncStatusResponse)
def get_sync_status(datasource_id: uuid.UUID, db: Session = Depends(get_db)):
    job = _SYNC_JOBS.get(str(datasource_id))
    if not job:
        ontology = _get_latest_ontology_optional(db, datasource_id)
        if ontology:
            data = json.loads(ontology.ontology_json or "{}")
            tables = data.get("tables") or []
            if tables:
                return OntologySyncStatusResponse(
                    status="completed",
                    total_tables=len(tables),
                    completed_tables=len(tables),
                    completed_at=(data.get("metadata") or {}).get("catalog_generated_at"),
                )
        return OntologySyncStatusResponse(status="idle")
    return OntologySyncStatusResponse(
        **{k: job.get(k) for k in OntologySyncStatusResponse.model_fields}
    )


@router.post("/datasources/{datasource_id}/sync", response_model=OntologySyncResponse, status_code=202)
def sync_ontology(datasource_id: uuid.UUID, background_tasks: BackgroundTasks):
    key = str(datasource_id)
    existing = _SYNC_JOBS.get(key)
    if existing and existing.get("status") == "running":
        return {"message": "Ontology synchronization already in progress."}

    _SYNC_JOBS[key] = {
        "status": "running",
        "total_tables": 0,
        "completed_tables": 0,
        "started_at": _now_iso(),
        "completed_at": None,
        "error": None,
    }
    background_tasks.add_task(_sync_ontology_background, datasource_id)
    return {"message": "Ontology synchronization started in the background."}

@router.get("/datasources/{datasource_id}/business-metrics", response_model=BusinessMetricsResponse)
def get_business_metrics(datasource_id: uuid.UUID, db: Session = Depends(get_db)):
    ontology = _get_latest_ontology(db, datasource_id)
    data = json.loads(ontology.ontology_json)
    metrics = data.get("business_metrics", [])
    return {"metrics": metrics}

@router.post("/datasources/{datasource_id}/business-metrics", response_model=BusinessMetric)
def add_business_metric(datasource_id: uuid.UUID, payload: BusinessMetric, db: Session = Depends(get_db)):
    latest = _get_latest_ontology(db, datasource_id)
    data = json.loads(latest.ontology_json)
    metrics = data.setdefault("business_metrics", [])
    
    # Check if exists, update or append
    existing = next((m for m in metrics if m.get("name") == payload.name), None)
    if existing:
        existing.update(payload.dict(exclude_unset=True))
        result = existing
    else:
        new_metric = payload.dict()
        metrics.append(new_metric)
        result = new_metric
        
    next_version = latest.version_number + 1
    new_ontology = OntologyVersionModel(
        datasource_connection_id=datasource_id,
        version_number=next_version,
        version_label=f"Metric Added v{next_version}",
        status="draft",
        ontology_json=json.dumps(data),
        graph_json=latest.graph_json
    )
    db.add(new_ontology)
    db.commit()
    db.refresh(new_ontology)
    
    return result


@router.get("/datasources/{datasource_id}/categories", response_model=OntologyCategoryResponse)
def get_categories(datasource_id: uuid.UUID, db: Session = Depends(get_db)):
    ontology = _get_latest_ontology(db, datasource_id)
    data = json.loads(ontology.ontology_json)
    
    categories = set()
    for table in data.get("tables", []):
        if table.get("category"):
            categories.add(table["category"])
            
    return {"categories": sorted(list(categories))}

@router.get("/datasources/{datasource_id}/tables", response_model=OntologyTableListResponse)
def get_tables(datasource_id: uuid.UUID, category: str = None, db: Session = Depends(get_db)):
    ontology = _get_latest_ontology(db, datasource_id)
    data = json.loads(ontology.ontology_json)
    
    tables = []
    for table in data.get("tables", []):
        if category and table.get("category") != category:
            continue

        columns = table.get("columns") or []
        confidence = table.get("confidence")
        if confidence is None:
            confidence = table.get("ai_confidence")

        tables.append(OntologyTableSummary(
            physical_name=table.get("physical_name", ""),
            category=table.get("category", "Unknown"),
            status=table.get("status", "PENDING_REVIEW"),
            is_ai_generated=bool(table.get("is_ai_generated", False)),
            confidence=confidence,
            description=table.get("description") or None,
            business_purpose=table.get("business_purpose") or None,
            business_concepts=table.get("business_concepts") if isinstance(table.get("business_concepts"), list) else None,
            common_questions=table.get("common_questions") if isinstance(table.get("common_questions"), list) else None,
            last_updated=table.get("last_updated") or None,
            tags=table.get("tags") if isinstance(table.get("tags"), list) else None,
            column_count=len(columns) if isinstance(columns, list) else None,
        ))
        
    return {"tables": tables}

@router.get("/tables/{table_name}/columns", response_model=OntologyColumnListResponse)
def get_columns(table_name: str, datasource_id: uuid.UUID, db: Session = Depends(get_db)):
    ontology = _get_latest_ontology(db, datasource_id)
    data = json.loads(ontology.ontology_json)
    
    columns = []
    for table in data.get("tables", []):
        if table.get("physical_name") == table_name:
            for col in table.get("columns", []):
                columns.append(OntologyColumnSummary(
                    physical_name=col.get("physical_name", ""),
                    semantic_type=col.get("semantic_type", "Unknown"),
                    business_definition=col.get("business_definition", ""),
                    status=col.get("status", "PENDING"),
                    confidence=col.get("confidence")
                ))
            break
            
    return {"columns": columns}


@router.post("/tables/{table_name}/generate-description", response_model=GenerateDescriptionResponse)
def generate_table_description(table_name: str, datasource_id: uuid.UUID, db: Session = Depends(get_db)):
    ontology = _get_latest_ontology(db, datasource_id)
    data = json.loads(ontology.ontology_json)
    
    target_table = None
    for table in data.get("tables", []):
        if table.get("physical_name") == table_name:
            target_table = table
            break
            
    if not target_table:
        raise HTTPException(status_code=404, detail="Table not found in ontology.")
        
    payload = {
        "physical_name": table_name,
        "columns": [{"name": c.get("physical_name")} for c in target_table.get("columns", [])]
    }
    
    conn = db.query(DatabaseConnectionModel).filter(DatabaseConnectionModel.id == datasource_id).first()
    project_id = str(conn.project_id) if conn and conn.project_id else None
    
    start_time = time.time()
    try:
        resp = requests.post(f"{LLM_ONTOLOGY_URL}/generate-table-description", json=payload)
        latency_ms = int((time.time() - start_time) * 1000)
        
        trace_payload = {
            "project_id": project_id,
            "ai_service": "ontology_generate_table_description",
            "latency_ms": latency_ms,
            "prompt_text": json.dumps(payload),
            "status": "success" if resp.status_code == 200 else "error",
            "error_message": resp.text if resp.status_code != 200 else None,
            "completion_text": resp.text if resp.status_code == 200 else None,
        }
        
        result = None
        if resp.status_code == 200:
            result = resp.json()
            trace_payload["prompt_tokens"] = result.get("prompt_tokens", 0)
            trace_payload["completion_tokens"] = result.get("completion_tokens", 0)
            trace_payload["model_name"] = result.get("model_name", "unknown")
            
        ingest_trace(db, trace_payload)
        
        if resp.status_code != 200:
            raise HTTPException(status_code=500, detail="LLM generation failed.")
            
    except Exception as e:
        if not isinstance(e, HTTPException):
            latency_ms = int((time.time() - start_time) * 1000)
            trace_payload = {
                "project_id": project_id,
                "ai_service": "ontology_generate_table_description",
                "latency_ms": latency_ms,
                "prompt_text": json.dumps(payload),
                "status": "error",
                "error_message": str(e),
            }
            ingest_trace(db, trace_payload)
        raise e
    
    target_table["description"] = result.get("description", "")
    target_table["category"] = result.get("category", "")
    if "confidence" in result:
        target_table["confidence"] = result.get("confidence")
    
    ontology.ontology_json = json.dumps(data)
    db.commit()
    
    return result

@router.post("/columns/{table_name}/{column_name}/generate-description", response_model=GenerateDescriptionResponse)
def generate_column_description(table_name: str, column_name: str, datasource_id: uuid.UUID, db: Session = Depends(get_db)):
    ontology = _get_latest_ontology(db, datasource_id)
    data = json.loads(ontology.ontology_json)
    
    target_col = None
    for table in data.get("tables", []):
        if table.get("physical_name") == table_name:
            for col in table.get("columns", []):
                if col.get("physical_name") == column_name:
                    target_col = col
                    break
            break
            
    if not target_col:
        raise HTTPException(status_code=404, detail="Column not found in ontology.")
        
    payload = {
        "table_name": table_name,
        "column_name": column_name
    }
    
    conn = db.query(DatabaseConnectionModel).filter(DatabaseConnectionModel.id == datasource_id).first()
    project_id = str(conn.project_id) if conn and conn.project_id else None
    
    start_time = time.time()
    try:
        resp = requests.post(f"{LLM_ONTOLOGY_URL}/generate-column-description", json=payload)
        latency_ms = int((time.time() - start_time) * 1000)
        
        trace_payload = {
            "project_id": project_id,
            "ai_service": "ontology_generate_column_description",
            "latency_ms": latency_ms,
            "prompt_text": json.dumps(payload),
            "status": "success" if resp.status_code == 200 else "error",
            "error_message": resp.text if resp.status_code != 200 else None,
            "completion_text": resp.text if resp.status_code == 200 else None,
        }
        
        result = None
        if resp.status_code == 200:
            result = resp.json()
            trace_payload["prompt_tokens"] = result.get("prompt_tokens", 0)
            trace_payload["completion_tokens"] = result.get("completion_tokens", 0)
            trace_payload["model_name"] = result.get("model_name", "unknown")
            
        ingest_trace(db, trace_payload)
        
        if resp.status_code != 200:
            raise HTTPException(status_code=500, detail="LLM generation failed.")
            
    except Exception as e:
        if not isinstance(e, HTTPException):
            latency_ms = int((time.time() - start_time) * 1000)
            trace_payload = {
                "project_id": project_id,
                "ai_service": "ontology_generate_column_description",
                "latency_ms": latency_ms,
                "prompt_text": json.dumps(payload),
                "status": "error",
                "error_message": str(e),
            }
            ingest_trace(db, trace_payload)
        raise e
    
    target_col["business_definition"] = result.get("business_definition", "")
    target_col["semantic_type"] = result.get("semantic_type", "")
    if "confidence" in result:
        target_col["confidence"] = result.get("confidence")
    
    ontology.ontology_json = json.dumps(data)
    db.commit()
    
    return result


@router.put("/tables/{table_name}")
def update_table(table_name: str, datasource_id: uuid.UUID, edit: TableOntologyEdit, db: Session = Depends(get_db)):
    ontology = _get_latest_ontology(db, datasource_id)
    data = json.loads(ontology.ontology_json)
    
    for table in data.get("tables", []):
        if table.get("physical_name") == table_name:
            table["description"] = edit.description
            table["category"] = edit.category
            table["status"] = edit.status
            break
            
    ontology.ontology_json = json.dumps(data)
    db.commit()
    return {"message": "Table updated."}

@router.put("/columns/{table_name}/{column_name}")
def update_column(table_name: str, column_name: str, datasource_id: uuid.UUID, edit: ColumnOntologyEdit, db: Session = Depends(get_db)):
    ontology = _get_latest_ontology(db, datasource_id)
    data = json.loads(ontology.ontology_json)
    
    for table in data.get("tables", []):
        if table.get("physical_name") == table_name:
            for col in table.get("columns", []):
                if col.get("physical_name") == column_name:
                    col["business_definition"] = edit.business_definition
                    col["semantic_type"] = edit.semantic_type
                    col["status"] = edit.status
                    break
            break
            
    ontology.ontology_json = json.dumps(data)
    db.commit()
    return {"message": "Column updated."}
