"""
duckdb_exporter.py - Exports filtered OMOP data into a DuckDB file.

Creates a DuckDB file with OMOP CDM 5.4 schema (same column names/types)
and inserts the filtered patient data. Schema mirrors PostgreSQL OMOP tables
but uses DuckDB-compatible types.
"""
import logging
import os
from datetime import datetime, date
from typing import Any, Dict, List, Optional

import duckdb

logger = logging.getLogger(__name__)

# DuckDB-compatible OMOP CDM 5.4 table DDLs
_TABLE_DDL = {
    "person": """
        CREATE TABLE IF NOT EXISTS person (
            person_id INTEGER PRIMARY KEY,
            gender_concept_id INTEGER NOT NULL,
            year_of_birth INTEGER NOT NULL,
            month_of_birth INTEGER,
            day_of_birth INTEGER,
            birth_datetime TIMESTAMP,
            race_concept_id INTEGER NOT NULL,
            ethnicity_concept_id INTEGER NOT NULL,
            location_id INTEGER,
            provider_id INTEGER,
            care_site_id INTEGER,
            person_source_value VARCHAR(50),
            gender_source_value VARCHAR(50),
            gender_source_concept_id INTEGER,
            race_source_value VARCHAR(50),
            race_source_concept_id INTEGER,
            ethnicity_source_value VARCHAR(50),
            ethnicity_source_concept_id INTEGER
        )
    """,
    "observation_period": """
        CREATE TABLE IF NOT EXISTS observation_period (
            observation_period_id INTEGER PRIMARY KEY,
            person_id INTEGER NOT NULL,
            observation_period_start_date DATE NOT NULL,
            observation_period_end_date DATE NOT NULL,
            period_type_concept_id INTEGER NOT NULL
        )
    """,
    "visit_occurrence": """
        CREATE TABLE IF NOT EXISTS visit_occurrence (
            visit_occurrence_id INTEGER PRIMARY KEY,
            person_id INTEGER NOT NULL,
            visit_concept_id INTEGER NOT NULL,
            visit_start_date DATE NOT NULL,
            visit_start_datetime TIMESTAMP,
            visit_end_date DATE NOT NULL,
            visit_end_datetime TIMESTAMP,
            visit_type_concept_id INTEGER NOT NULL,
            provider_id INTEGER,
            care_site_id INTEGER,
            visit_source_value VARCHAR(50),
            visit_source_concept_id INTEGER,
            admitted_from_concept_id INTEGER,
            admitted_from_source_value VARCHAR(50),
            discharged_to_concept_id INTEGER,
            discharged_to_source_value VARCHAR(50),
            preceding_visit_occurrence_id INTEGER
        )
    """,
    "visit_detail": """
        CREATE TABLE IF NOT EXISTS visit_detail (
            visit_detail_id INTEGER PRIMARY KEY,
            person_id INTEGER NOT NULL,
            visit_detail_concept_id INTEGER NOT NULL,
            visit_detail_start_date DATE NOT NULL,
            visit_detail_start_datetime TIMESTAMP,
            visit_detail_end_date DATE NOT NULL,
            visit_detail_end_datetime TIMESTAMP,
            visit_detail_type_concept_id INTEGER NOT NULL,
            provider_id INTEGER,
            care_site_id INTEGER,
            visit_detail_source_value VARCHAR(50),
            visit_detail_source_concept_id INTEGER,
            admitted_from_concept_id INTEGER,
            admitted_from_source_value VARCHAR(50),
            discharged_to_concept_id INTEGER,
            discharged_to_source_value VARCHAR(50),
            preceding_visit_detail_id INTEGER,
            visit_detail_parent_id INTEGER,
            visit_occurrence_id INTEGER NOT NULL
        )
    """,
    "condition_occurrence": """
        CREATE TABLE IF NOT EXISTS condition_occurrence (
            condition_occurrence_id INTEGER PRIMARY KEY,
            person_id INTEGER NOT NULL,
            condition_concept_id INTEGER NOT NULL,
            condition_start_date DATE NOT NULL,
            condition_start_datetime TIMESTAMP,
            condition_end_date DATE,
            condition_end_datetime TIMESTAMP,
            condition_type_concept_id INTEGER NOT NULL,
            condition_status_concept_id INTEGER,
            stop_reason VARCHAR(20),
            provider_id INTEGER,
            visit_occurrence_id INTEGER,
            visit_detail_id INTEGER,
            condition_source_value VARCHAR(50),
            condition_source_concept_id INTEGER,
            condition_status_source_value VARCHAR(50)
        )
    """,
    "drug_exposure": """
        CREATE TABLE IF NOT EXISTS drug_exposure (
            drug_exposure_id INTEGER PRIMARY KEY,
            person_id INTEGER NOT NULL,
            drug_concept_id INTEGER NOT NULL,
            drug_exposure_start_date DATE NOT NULL,
            drug_exposure_start_datetime TIMESTAMP,
            drug_exposure_end_date DATE NOT NULL,
            drug_exposure_end_datetime TIMESTAMP,
            verbatim_end_date DATE,
            drug_type_concept_id INTEGER NOT NULL,
            stop_reason VARCHAR(20),
            refills INTEGER,
            quantity DOUBLE,
            days_supply INTEGER,
            sig VARCHAR,
            route_concept_id INTEGER,
            lot_number VARCHAR(50),
            provider_id INTEGER,
            visit_occurrence_id INTEGER,
            visit_detail_id INTEGER,
            drug_source_value VARCHAR(50),
            drug_source_concept_id INTEGER,
            route_source_value VARCHAR(50),
            dose_unit_source_value VARCHAR(50)
        )
    """,
    "procedure_occurrence": """
        CREATE TABLE IF NOT EXISTS procedure_occurrence (
            procedure_occurrence_id INTEGER PRIMARY KEY,
            person_id INTEGER NOT NULL,
            procedure_concept_id INTEGER NOT NULL,
            procedure_date DATE NOT NULL,
            procedure_datetime TIMESTAMP,
            procedure_end_date DATE,
            procedure_end_datetime TIMESTAMP,
            procedure_type_concept_id INTEGER NOT NULL,
            modifier_concept_id INTEGER,
            quantity INTEGER,
            provider_id INTEGER,
            visit_occurrence_id INTEGER,
            visit_detail_id INTEGER,
            procedure_source_value VARCHAR(50),
            procedure_source_concept_id INTEGER,
            modifier_source_value VARCHAR(50)
        )
    """,
    "device_exposure": """
        CREATE TABLE IF NOT EXISTS device_exposure (
            device_exposure_id INTEGER PRIMARY KEY,
            person_id INTEGER NOT NULL,
            device_concept_id INTEGER NOT NULL,
            device_exposure_start_date DATE NOT NULL,
            device_exposure_start_datetime TIMESTAMP,
            device_exposure_end_date DATE,
            device_exposure_end_datetime TIMESTAMP,
            device_type_concept_id INTEGER NOT NULL,
            unique_device_id VARCHAR(255),
            production_id VARCHAR(255),
            quantity INTEGER,
            provider_id INTEGER,
            visit_occurrence_id INTEGER,
            visit_detail_id INTEGER,
            device_source_value VARCHAR(50),
            device_source_concept_id INTEGER,
            unit_concept_id INTEGER,
            unit_source_value VARCHAR(50),
            unit_source_concept_id INTEGER
        )
    """,
    "measurement": """
        CREATE TABLE IF NOT EXISTS measurement (
            measurement_id INTEGER PRIMARY KEY,
            person_id INTEGER NOT NULL,
            measurement_concept_id INTEGER NOT NULL,
            measurement_date DATE NOT NULL,
            measurement_datetime TIMESTAMP,
            measurement_time VARCHAR(10),
            measurement_type_concept_id INTEGER NOT NULL,
            operator_concept_id INTEGER,
            value_as_number DOUBLE,
            value_as_concept_id INTEGER,
            unit_concept_id INTEGER,
            range_low DOUBLE,
            range_high DOUBLE,
            provider_id INTEGER,
            visit_occurrence_id INTEGER,
            visit_detail_id INTEGER,
            measurement_source_value VARCHAR(50),
            measurement_source_concept_id INTEGER,
            unit_source_value VARCHAR(50),
            unit_source_concept_id INTEGER,
            value_source_value VARCHAR(50),
            measurement_event_id INTEGER,
            meas_event_field_concept_id INTEGER
        )
    """,
    "observation": """
        CREATE TABLE IF NOT EXISTS observation (
            observation_id INTEGER PRIMARY KEY,
            person_id INTEGER NOT NULL,
            observation_concept_id INTEGER NOT NULL,
            observation_date DATE NOT NULL,
            observation_datetime TIMESTAMP,
            observation_type_concept_id INTEGER NOT NULL,
            value_as_number DOUBLE,
            value_as_string VARCHAR(60),
            value_as_concept_id INTEGER,
            qualifier_concept_id INTEGER,
            unit_concept_id INTEGER,
            provider_id INTEGER,
            visit_occurrence_id INTEGER,
            visit_detail_id INTEGER,
            observation_source_value VARCHAR(50),
            observation_source_concept_id INTEGER,
            unit_source_value VARCHAR(50),
            qualifier_source_value VARCHAR(50),
            value_source_value VARCHAR(50),
            observation_event_id INTEGER,
            obs_event_field_concept_id INTEGER
        )
    """,
    "death": """
        CREATE TABLE IF NOT EXISTS death (
            person_id INTEGER NOT NULL,
            death_date DATE NOT NULL,
            death_datetime TIMESTAMP,
            death_type_concept_id INTEGER,
            cause_concept_id INTEGER,
            cause_source_value VARCHAR(50),
            cause_source_concept_id INTEGER
        )
    """,
    "note": """
        CREATE TABLE IF NOT EXISTS note (
            note_id INTEGER PRIMARY KEY,
            person_id INTEGER NOT NULL,
            note_date DATE NOT NULL,
            note_datetime TIMESTAMP,
            note_type_concept_id INTEGER NOT NULL,
            note_class_concept_id INTEGER NOT NULL,
            note_title VARCHAR(250),
            note_text VARCHAR,
            encoding_concept_id INTEGER NOT NULL,
            language_concept_id INTEGER NOT NULL,
            provider_id INTEGER,
            visit_occurrence_id INTEGER,
            visit_detail_id INTEGER,
            note_source_value VARCHAR(50),
            note_event_id INTEGER,
            note_event_field_concept_id INTEGER
        )
    """,
    "specimen": """
        CREATE TABLE IF NOT EXISTS specimen (
            specimen_id INTEGER PRIMARY KEY,
            person_id INTEGER NOT NULL,
            specimen_concept_id INTEGER NOT NULL,
            specimen_type_concept_id INTEGER NOT NULL,
            specimen_date DATE NOT NULL,
            specimen_datetime TIMESTAMP,
            quantity DOUBLE,
            unit_concept_id INTEGER,
            anatomic_site_concept_id INTEGER,
            disease_status_concept_id INTEGER,
            specimen_source_id VARCHAR(50),
            specimen_source_value VARCHAR(50),
            unit_source_value VARCHAR(50),
            anatomic_site_source_value VARCHAR(50),
            disease_status_source_value VARCHAR(50)
        )
    """,
    "payer_plan_period": """
        CREATE TABLE IF NOT EXISTS payer_plan_period (
            payer_plan_period_id INTEGER PRIMARY KEY,
            person_id INTEGER NOT NULL,
            payer_plan_period_start_date DATE NOT NULL,
            payer_plan_period_end_date DATE NOT NULL,
            payer_concept_id INTEGER,
            payer_source_value VARCHAR(50),
            payer_source_concept_id INTEGER,
            plan_concept_id INTEGER,
            plan_source_value VARCHAR(50),
            plan_source_concept_id INTEGER,
            sponsor_concept_id INTEGER,
            sponsor_source_value VARCHAR(50),
            sponsor_source_concept_id INTEGER,
            family_source_value VARCHAR(50),
            stop_reason_concept_id INTEGER,
            stop_reason_source_value VARCHAR(50),
            stop_reason_source_concept_id INTEGER
        )
    """,
    "condition_era": """
        CREATE TABLE IF NOT EXISTS condition_era (
            condition_era_id INTEGER PRIMARY KEY,
            person_id INTEGER NOT NULL,
            condition_concept_id INTEGER NOT NULL,
            condition_era_start_date DATE NOT NULL,
            condition_era_end_date DATE NOT NULL,
            condition_occurrence_count INTEGER
        )
    """,
    "drug_era": """
        CREATE TABLE IF NOT EXISTS drug_era (
            drug_era_id INTEGER PRIMARY KEY,
            person_id INTEGER NOT NULL,
            drug_concept_id INTEGER NOT NULL,
            drug_era_start_date DATE NOT NULL,
            drug_era_end_date DATE NOT NULL,
            drug_exposure_count INTEGER,
            gap_days INTEGER
        )
    """,
    "dose_era": """
        CREATE TABLE IF NOT EXISTS dose_era (
            dose_era_id INTEGER PRIMARY KEY,
            person_id INTEGER NOT NULL,
            drug_concept_id INTEGER NOT NULL,
            unit_concept_id INTEGER NOT NULL,
            dose_value DOUBLE NOT NULL,
            dose_era_start_date DATE NOT NULL,
            dose_era_end_date DATE NOT NULL
        )
    """,
    "episode": """
        CREATE TABLE IF NOT EXISTS episode (
            episode_id INTEGER PRIMARY KEY,
            person_id INTEGER NOT NULL,
            episode_concept_id INTEGER NOT NULL,
            episode_start_date DATE NOT NULL,
            episode_start_datetime TIMESTAMP,
            episode_end_date DATE,
            episode_end_datetime TIMESTAMP,
            episode_parent_id INTEGER,
            episode_number INTEGER,
            episode_object_concept_id INTEGER NOT NULL,
            episode_type_concept_id INTEGER NOT NULL,
            episode_source_value VARCHAR(50),
            episode_source_concept_id INTEGER
        )
    """,
    "concept": """
        CREATE TABLE IF NOT EXISTS concept (
            concept_id INTEGER PRIMARY KEY,
            concept_name VARCHAR(255) NOT NULL,
            domain_id VARCHAR(20) NOT NULL,
            vocabulary_id VARCHAR(20) NOT NULL,
            concept_class_id VARCHAR(20) NOT NULL,
            standard_concept VARCHAR(1),
            concept_code VARCHAR(50) NOT NULL,
            valid_start_date DATE NOT NULL,
            valid_end_date DATE NOT NULL,
            invalid_reason VARCHAR(1)
        )
    """,
    "concept_relationship": """
        CREATE TABLE IF NOT EXISTS concept_relationship (
            concept_id_1 INTEGER NOT NULL,
            concept_id_2 INTEGER NOT NULL,
            relationship_id VARCHAR(20) NOT NULL,
            valid_start_date DATE NOT NULL,
            valid_end_date DATE NOT NULL,
            invalid_reason VARCHAR(1)
        )
    """,
}

# All person-scoped OMOP tables
PERSON_SCOPED_TABLES = [
    "person",
    "observation_period",
    "visit_occurrence",
    "visit_detail",
    "condition_occurrence",
    "drug_exposure",
    "procedure_occurrence",
    "device_exposure",
    "measurement",
    "observation",
    "death",
    "note",
    "specimen",
    "payer_plan_period",
    "condition_era",
    "drug_era",
    "dose_era",
    "episode",
]


class DuckDBExporter:
    """
    Exports filtered OMOP data into a DuckDB file.

    Usage::

        exporter = DuckDBExporter()
        path = exporter.export(data, "/tmp/output/cohort_filter.duckdb")
    """

    def export(
        self,
        data: Dict[str, List[Dict[str, Any]]],
        output_path: str,
    ) -> str:
        """
        Create a DuckDB file with OMOP CDM schema and insert filtered data.

        Args:
            data: Dict of {table_name: [row_dicts]} from the filter query.
            output_path: Full path for the output .duckdb file.

        Returns:
            Absolute path to the created .duckdb file.
        """
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)

        # Remove existing file so we start fresh
        if os.path.exists(output_path):
            os.remove(output_path)

        conn = duckdb.connect(output_path)
        try:
            self._create_schema(conn, data)
            total_rows = self._insert_data(conn, data)
            logger.info(
                f"DuckDB export complete: {output_path} "
                f"({len(data)} tables, {total_rows:,} total rows)"
            )
        finally:
            conn.close()

        return os.path.abspath(output_path)

    def _create_schema(self, conn: duckdb.DuckDBPyConnection, data: Dict) -> None:
        """Create DDL for all tables that have data."""
        for table_name in data.keys():
            ddl = _TABLE_DDL.get(table_name)
            if ddl:
                conn.execute(ddl)
                logger.debug(f"Created table: {table_name}")
            else:
                # Generic fallback: infer columns from first row
                rows = data[table_name]
                if rows:
                    self._create_generic_table(conn, table_name, rows[0])

    def _create_generic_table(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        sample_row: Dict[str, Any],
    ) -> None:
        """Create a generic table DDL inferred from a sample row."""
        cols = []
        for col, val in sample_row.items():
            if isinstance(val, int):
                col_type = "INTEGER"
            elif isinstance(val, float):
                col_type = "DOUBLE"
            elif isinstance(val, (date, datetime)):
                col_type = "TIMESTAMP"
            else:
                col_type = "VARCHAR"
            cols.append(f"{col} {col_type}")
        ddl = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(cols)})"
        conn.execute(ddl)

    def _insert_data(
        self, conn: duckdb.DuckDBPyConnection, data: Dict[str, List[Dict[str, Any]]]
    ) -> int:
        """Insert all rows into DuckDB tables. Returns total row count."""
        total = 0
        for table_name, rows in data.items():
            if not rows:
                continue
            inserted = self._insert_table(conn, table_name, rows)
            total += inserted
            logger.info(f"  Inserted {inserted} rows into {table_name}")
        return total

    def _insert_table(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        rows: List[Dict[str, Any]],
    ) -> int:
        """Insert rows for a single table using parameterized batch insert."""
        if not rows:
            return 0

        columns = list(rows[0].keys())
        col_str = ", ".join(columns)
        placeholders = ", ".join(["?" for _ in columns])
        sql = f"INSERT OR IGNORE INTO {table_name} ({col_str}) VALUES ({placeholders})"

        batch = []
        for row in rows:
            values = []
            for col in columns:
                val = row.get(col)
                # Convert date/datetime to string for DuckDB compatibility
                if isinstance(val, datetime):
                    val = val.isoformat()
                elif isinstance(val, date):
                    val = val.isoformat()
                values.append(val)
            batch.append(values)

        try:
            conn.executemany(sql, batch)
            return len(batch)
        except Exception as exc:
            logger.warning(f"Batch insert failed for {table_name}: {exc}, trying row-by-row")
            salvaged = 0
            for row_vals in batch:
                try:
                    conn.execute(sql, row_vals)
                    salvaged += 1
                except Exception as row_exc:
                    logger.debug(f"Row skip in {table_name}: {row_exc}")
            return salvaged
