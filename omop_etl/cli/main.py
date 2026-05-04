#!/usr/bin/env python3
"""
cli/main.py - OMOP ETL Command-Line Interface.

All config values flow through ETLConfig. The CLI is a thin adapter
that collects user inputs and constructs the config object.
"""
import os
import sys
import logging
import traceback

import click
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import DBConfig, ETLConfig
from core.profiler import profile_csv
from core.concept_resolver import ConceptResolver, parse_mapping_file
from core.domain_classifier import DomainClassifier
from loaders.vocab_loader import load_vocabulary
from pipeline import OMOPPipeline


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level   = getattr(logging, level.upper(), logging.INFO),
        format  = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers = [
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("omop_etl.log", mode="a"),
        ],
    )


# ---------------------------------------------------------------------------
# Shared DB options decorator
# ---------------------------------------------------------------------------

def db_options(f):
    for opt in reversed([
        click.option("--host",     default="localhost", show_default=True,
                     envvar="OMOP_HOST",     help="PostgreSQL host"),
        click.option("--port",     default=5432,        show_default=True,
                     envvar="OMOP_PORT",     help="PostgreSQL port"),
        click.option("--dbname",   default="omop",      show_default=True,
                     envvar="OMOP_DB",       help="Database name"),
        click.option("--user",     default="postgres",  show_default=True,
                     envvar="OMOP_USER",     help="DB user"),
        click.option("--password", default="omop",      show_default=True,
                     envvar="OMOP_PASSWORD", help="DB password"),
        click.option("--schema",   default="public",    show_default=True,
                     envvar="OMOP_SCHEMA",   help="OMOP schema name"),
    ]):
        f = opt(f)
    return f


def _make_db_config(host, port, dbname, user, password, schema) -> DBConfig:
    return DBConfig(host=host, port=port, dbname=dbname,
                    user=user, password=password, schema=schema)


def _connect(host, port, dbname, user, password):
    try:
        conn = psycopg2.connect(
            host=host, port=port, dbname=dbname, user=user, password=password
        )
        click.echo(f"✓ Connected to {host}:{port}/{dbname}")
        return conn
    except Exception as exc:
        click.echo(f"✗ DB connection failed: {exc}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------

@click.group()
@click.option(
    "--log-level", default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    help="Logging verbosity",
)
def cli(log_level):
    """
    \b
    ╔══════════════════════════════════════════╗
    ║   OMOP ETL — Dynamic CSV → OMOP CDM     ║
    ╚══════════════════════════════════════════╝
    """
    setup_logging(log_level)


# ---------------------------------------------------------------------------
# load-vocab
# ---------------------------------------------------------------------------

@cli.command("load-vocab")
@click.option("--vocab-dir", "-v", required=True,
              type=click.Path(exists=True, file_okay=False),
              help="Athena vocabulary directory (containing CONCEPT.csv etc.)")
@click.option("--tables", "-t", multiple=True,
              help="Specific tables to load (default: all)")
@db_options
def load_vocab(vocab_dir, tables, host, port, dbname, user, password, schema):
    """Load Athena vocabulary CSVs into OMOP CDM schema."""
    click.echo(f"\n📚 Loading vocabulary from: {vocab_dir}")
    conn        = _connect(host, port, dbname, user, password)
    tables_list = list(tables) if tables else None
    try:
        load_vocabulary(vocab_dir, conn, schema=schema, tables=tables_list)
        click.echo("✅ Vocabulary loaded successfully!\n")
    except Exception as exc:
        click.echo(f"✗ Vocabulary load failed: {exc}", err=True)
        sys.exit(1)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# profile
# ---------------------------------------------------------------------------

@cli.command("profile")
@click.option("--csv", "-c", "csv_path", required=True,
              type=click.Path(exists=True), help="Source CSV file path")
@click.option("--mapping", "-m", "mapping_paths", multiple=True,
              type=click.Path(exists=True),
              help="SNOMED mapping CSV file(s). Can be specified multiple times.")
@click.option("--person-id-column", default=None,
              help="Explicit source column to use as the OMOP person identifier")
@click.option("--allow-uuid-person-id", is_flag=True,
              help="Allow UUID-like submission IDs to become OMOP person IDs")
@click.option("--output", "-o", default=None, help="Save profile report to file")
def profile_cmd(csv_path, mapping_paths, person_id_column, allow_uuid_person_id, output):
    """Profile a CSV dataset — show columns, domains, SNOMED coverage."""
    click.echo(f"\n🔍 Profiling: {csv_path}")

    profile = profile_csv(
        csv_path,
        mapping_paths        = list(mapping_paths),
        person_id_column     = person_id_column,
        allow_uuid_person_id = allow_uuid_person_id,
    )

    field_snomed_map = {}
    for mp in mapping_paths:
        field_snomed_map.update(parse_mapping_file(mp))

    total  = len(profile.columns)
    mapped = sum(1 for c in profile.columns if c in field_snomed_map)

    from collections import Counter
    domains = Counter(cp.domain_hint for cp in profile.columns.values())
    dtypes  = Counter(cp.dtype       for cp in profile.columns.values())

    lines = [
        "",
        "═" * 60,
        f"  DATASET PROFILE: {os.path.basename(csv_path)}",
        "═" * 60,
        f"  Rows:              {profile.n_rows:,}",
        f"  Columns:           {total:,}",
        f"  SNOMED-mapped:     {mapped:,} ({mapped / total * 100:.1f}%)",
        f"  Person ID col:     {profile.person_id_col or 'NOT DETECTED'}",
        f"  ID candidates:     {', '.join(profile.person_id_candidates) or 'NONE'}",
        f"  Gender col:        {profile.gender_col or 'NOT DETECTED'}",
        f"  Age col:           {profile.age_col or 'NOT DETECTED'}",
        f"  Birth date col:    {profile.birth_date_col or 'NOT DETECTED'}",
        f"  Date cols:         {len(profile.date_cols)} detected",
        f"  Groups (prefixes): {len(profile.groups)}",
        "─" * 60,
        "  Domain distribution (heuristic):",
        *[f"    {d:<20} {c:>5} columns" for d, c in domains.most_common()],
        "",
        "  Data type distribution:",
        *[f"    {dt:<20} {c:>5} columns" for dt, c in dtypes.most_common()],
        "═" * 60,
    ]

    unmapped = [c for c in profile.columns if c not in field_snomed_map][:20]
    if unmapped:
        lines.append(f"\n  Sample unmapped columns ({len(unmapped)} shown):")
        for c in unmapped:
            cp = profile.columns[c]
            lines.append(f"    {c[:50]:<52} dtype={cp.dtype}, domain_hint={cp.domain_hint}")

    report = "\n".join(lines)
    click.echo(report)
    if output:
        with open(output, "w") as f:
            f.write(report)
        click.echo(f"\n📄 Report saved to: {output}")


# ---------------------------------------------------------------------------
# inspect
# ---------------------------------------------------------------------------

@cli.command("inspect")
@click.option("--csv", "-c", "csv_path", required=True,
              type=click.Path(exists=True))
@click.option("--mapping", "-m", "mapping_paths", multiple=True,
              type=click.Path(exists=True))
@click.option("--person-id-column", default=None)
@click.option("--allow-uuid-person-id", is_flag=True)
@click.option("--limit", default=50, show_default=True,
              help="Number of classified columns to display")
@db_options
def inspect_cmd(csv_path, mapping_paths, person_id_column, allow_uuid_person_id,
                limit, host, port, dbname, user, password, schema):
    """Inspect column-level OMOP routing for a CSV + mapping files."""
    conn     = _connect(host, port, dbname, user, password)
    resolver = ConceptResolver(conn)

    profile = profile_csv(
        csv_path,
        mapping_paths        = list(mapping_paths),
        resolver             = resolver,
        person_id_column     = person_id_column,
        allow_uuid_person_id = allow_uuid_person_id,
    )

    field_snomed_map = {}
    for mp in mapping_paths:
        field_snomed_map.update(parse_mapping_file(mp))

    classifier = DomainClassifier(resolver, field_snomed_map)
    routes     = classifier.classify_all(profile)

    click.echo(f"\n{'Column':<55} {'Table':<25} {'Concept':>10}  Strategy")
    click.echo("─" * 110)

    shown = 0
    for col_name, route in routes.items():
        if route.value_strategy == "skip":
            continue
        click.echo(
            f"{col_name[:54]:<55} {route.target_table:<25} "
            f"{route.concept_id:>10}  {route.value_strategy}"
        )
        shown += 1
        if shown >= limit:
            remaining = sum(1 for r in routes.values() if r.value_strategy != "skip") - shown
            click.echo(f"  … {remaining} more columns not shown (increase --limit)")
            break

    conn.close()
    click.echo("")


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

@cli.command("run")
@click.option("--csv", "-c", "csv_path", required=True,
              type=click.Path(exists=True), help="Source CSV file")
@click.option("--mapping", "-m", "mapping_paths", multiple=True,
              type=click.Path(exists=True),
              help="SNOMED mapping CSV file(s)")
@click.option("--source-name", "-s", default="UNKNOWN_SOURCE", show_default=True,
              help="Source dataset name (used in person_source_value)")
@click.option("--batch-size", default=1_000, show_default=True)
@click.option("--dry-run", is_flag=True, help="Validate without writing to DB")
@click.option("--person-id-column", default=None)
@click.option("--allow-uuid-person-id",    is_flag=True)
@click.option("--allow-synthetic-person-id", is_flag=True,
              help="Generate one synthetic person per CSV row")
@db_options
def run_cmd(
    csv_path, mapping_paths, source_name, batch_size, dry_run,
    person_id_column, allow_uuid_person_id, allow_synthetic_person_id,
    host, port, dbname, user, password, schema,
):
    """Run full ETL: CSV → OMOP CDM."""
    click.echo(f"\n🚀 OMOP ETL")
    click.echo(f"   CSV:    {csv_path}")
    click.echo(f"   Source: {source_name}")
    click.echo(f"   DB:     {host}:{port}/{dbname} (schema={schema})")
    if dry_run:
        click.echo("   ⚠  DRY RUN — no data will be written")
    click.echo("")

    # Quick profile preview to catch person-ID issues early
    try:
        preview = profile_csv(
            csv_path,
            mapping_paths        = list(mapping_paths),
            person_id_column     = person_id_column,
            allow_uuid_person_id = allow_uuid_person_id,
        )
    except RuntimeError as exc:
        click.echo(f"✗ {exc}", err=True)
        sys.exit(1)

    if not preview.person_id_col and not allow_synthetic_person_id and person_id_column is None:
        click.echo("⚠️  No stable source person identifier was detected.")
        if preview.person_id_candidates:
            click.echo(f"   Top candidate: {preview.person_id_candidates[0]}")
        allow_synthetic_person_id = click.confirm(
            "Generate one synthetic OMOP person per CSV row and continue?",
            default=False,
        )

    if not dry_run:
        click.confirm("Proceed with ETL?", abort=True)

    config = ETLConfig(
        db                         = _make_db_config(host, port, dbname, user, password, schema),
        csv_path                   = csv_path,
        mapping_paths              = list(mapping_paths),
        batch_size                 = batch_size,
        dry_run                    = dry_run,
        source_name                = source_name,
        person_id_column           = person_id_column,
        allow_uuid_person_id       = allow_uuid_person_id,
        allow_synthetic_person_id  = allow_synthetic_person_id,
    )

    pipeline = OMOPPipeline(config)

    with click.progressbar(length=5, label="ETL Progress",
                           item_show_func=lambda x: x or "") as bar:
        def progress(current, total, msg):
            bar.label = msg
            if current > 0:
                bar.update(1)
        try:
            pipeline.run(progress_callback=progress)
        except ConnectionError as exc:
            click.echo(f"\n✗ {exc}", err=True)
            sys.exit(1)
        except Exception as exc:
            click.echo(f"\n✗ ETL failed: {exc}", err=True)
            traceback.print_exc()
            sys.exit(1)

    pipeline.writer.print_summary()
    click.echo("✅ ETL complete! Check omop_etl.log for details.\n")


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------

@cli.command("verify")
@db_options
def verify_cmd(host, port, dbname, user, password, schema):
    """Verify OMOP CDM tables exist and show row counts."""
    conn = _connect(host, port, dbname, user, password)

    # Table list is the CDM spec — appropriate to define here, not in config
    omop_tables = [
        "person", "observation_period", "visit_occurrence",
        "condition_occurrence", "drug_exposure", "measurement",
        "observation", "procedure_occurrence", "death",
        "concept", "concept_relationship", "vocabulary",
    ]

    click.echo(f"\n📊 OMOP CDM Table Counts ({host}/{dbname}):\n")
    cur = conn.cursor()
    for table in omop_tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
            count  = cur.fetchone()[0]
            status = "✓" if count > 0 else "○"
            click.echo(f"  {status} {table:<35} {count:>12,} rows")
        except Exception:
            click.echo(f"  ✗ {table:<35} {'NOT FOUND':>12}")
    cur.close()
    conn.close()
    click.echo("")


# ---------------------------------------------------------------------------
# wizard
# ---------------------------------------------------------------------------

@cli.command("wizard")
def wizard_cmd():
    """Interactive setup wizard — guides you through first-time configuration."""
    click.echo("\n" + "═" * 60)
    click.echo("  OMOP ETL SETUP WIZARD")
    click.echo("═" * 60 + "\n")

    host     = click.prompt("PostgreSQL host",    default="localhost")
    port     = click.prompt("Port",               default=5432, type=int)
    dbname   = click.prompt("Database name",      default="omop")
    user     = click.prompt("Username",           default="postgres")
    password = click.prompt("Password",           default="omop", hide_input=True)
    schema   = click.prompt("Schema",             default="public")

    click.echo("\n  Testing connection…")
    try:
        psycopg2.connect(
            host=host, port=port, dbname=dbname, user=user, password=password
        ).close()
        click.echo("  ✓ Connection successful!")
    except Exception as exc:
        click.echo(f"  ✗ Connection failed: {exc}")
        sys.exit(1)

    if click.confirm("\nLoad Athena vocabulary now?", default=True):
        vocab_dir = click.prompt("Athena vocab directory path")
        if os.path.isdir(vocab_dir):
            conn = psycopg2.connect(
                host=host, port=port, dbname=dbname, user=user, password=password
            )
            load_vocabulary(vocab_dir, conn, schema=schema)
            conn.close()
            click.echo("  ✓ Vocabulary loaded!")
        else:
            click.echo(f"  ⚠ Directory not found: {vocab_dir}")

    csv_path = click.prompt("\nSource CSV path")
    mappings = []
    while True:
        mp = click.prompt("SNOMED mapping CSV path (blank to finish)", default="")
        if not mp:
            break
        if os.path.exists(mp):
            mappings.append(mp)
        else:
            click.echo(f"  ⚠ File not found: {mp}")

    source_name = click.prompt("Source dataset name", default="dataset")
    dry_run     = click.confirm("Dry run first?", default=True)

    config = ETLConfig(
        db            = _make_db_config(host, port, dbname, user, password, schema),
        csv_path      = csv_path,
        mapping_paths = mappings,
        dry_run       = dry_run,
        source_name   = source_name,
    )

    if click.confirm("\nRun ETL now?", default=True):
        pipeline = OMOPPipeline(config)
        try:
            pipeline.run()
            pipeline.writer.print_summary()
        except Exception as exc:
            click.echo(f"✗ ETL failed: {exc}", err=True)


# ---------------------------------------------------------------------------
# create-cohort
# ---------------------------------------------------------------------------

@cli.command("create-cohort")
@click.option("--name", "-n", "cohort_name", required=True,
              help="Human-readable name for this cohort")
@click.option("--description", "-d", "cohort_description", default="",
              help="Free-text description of the cohort")
@click.option("--where", "-w", "where_clause", default=None,
              help="Optional SQL WHERE predicate on the person table "
                   "(e.g. 'year_of_birth > 1950')")
@db_options
def create_cohort_cmd(cohort_name, cohort_description, where_clause,
                      host, port, dbname, user, password, schema):
    """Create an OMOP cohort from already-ingested person data."""
    from core.cohort_builder import CohortBuilder
    from core.id_generator import OMOPIdGenerator

    click.echo(f"\n👥 Creating cohort: '{cohort_name}'")
    if where_clause:
        click.echo(f"   Filter: WHERE {where_clause}")
    click.echo(f"   DB: {host}:{port}/{dbname} (schema={schema})")
    click.echo("")

    conn = _connect(host, port, dbname, user, password)

    try:
        id_gen = OMOPIdGenerator(conn, schema=schema)
        cohort_id = id_gen.next_id("cohort")

        builder = CohortBuilder(conn, schema=schema)
        meta = builder.build_cohort(
            cohort_definition_id=cohort_id,
            cohort_name=cohort_name,
            cohort_description=cohort_description,
            where_clause=where_clause,
        )

        click.echo("═" * 50)
        click.echo(f"  ✅ Cohort created successfully!")
        click.echo(f"  Cohort ID:     {meta.cohort_definition_id}")
        click.echo(f"  Name:          {meta.cohort_name}")
        click.echo(f"  Persons:       {meta.person_count:,}")
        click.echo(f"  Date range:    {meta.earliest_start} → {meta.latest_end}")
        click.echo("═" * 50)
        click.echo(f"\n  Use --cohort-id {meta.cohort_definition_id} "
                   f"with 'export-cohort' to extract this cohort.\n")

    except Exception as exc:
        click.echo(f"✗ Cohort creation failed: {exc}", err=True)
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# export-cohort
# ---------------------------------------------------------------------------

@cli.command("export-cohort")
@click.option("--cohort-id", "-c", "cohort_id", required=True, type=int,
              help="Cohort definition ID to export")
@click.option("--output-dir", "-o", required=True,
              type=click.Path(file_okay=False),
              help="Directory where the zip bundle will be saved")
@click.option("--cohort-name", default="",
              help="Cohort name (stamped in manifest)")
@click.option("--cohort-description", default="",
              help="Cohort description (stamped in manifest)")
@db_options
def export_cohort_cmd(cohort_id, output_dir, cohort_name, cohort_description,
                      host, port, dbname, user, password, schema):
    """Export cohort-scoped OMOP data + vocabulary subset as a .zip bundle."""
    from loaders.cohort_extractor import CohortExtractor
    from loaders.bundle_exporter import BundleExporter

    click.echo(f"\n📦 Exporting cohort {cohort_id}")
    click.echo(f"   Output: {output_dir}")
    click.echo(f"   DB:     {host}:{port}/{dbname} (schema={schema})")
    click.echo("")

    conn = _connect(host, port, dbname, user, password)

    try:
        extractor = CohortExtractor(conn, schema=schema)
        click.echo("Extracting cohort data...")
        data = extractor.extract(cohort_definition_id=cohort_id)

        if not data:
            click.echo("⚠  No data found for this cohort. "
                       "Did you run 'create-cohort' first?", err=True)
            sys.exit(1)

        exporter = BundleExporter(conn, schema=schema)
        zip_path = exporter.export(
            data=data,
            output_dir=output_dir,
            cohort_definition_id=cohort_id,
            cohort_name=cohort_name,
            cohort_description=cohort_description,
            source_schema=schema,
        )

        # Summary
        table_counts = {t: len(rows) for t, rows in data.items() if rows}
        person_count = len(data.get("person", []))

        click.echo("\n" + "═" * 60)
        click.echo("  COHORT EXPORT SUMMARY")
        click.echo("═" * 60)
        for table, count in sorted(table_counts.items()):
            click.echo(f"  ✓ {table:<35} {count:>8,} rows")
        click.echo("─" * 60)
        click.echo(f"  Persons:      {person_count:,}")
        click.echo(f"  Tables:       {len(table_counts):,}")
        click.echo(f"  Total rows:   {sum(table_counts.values()):,}")
        click.echo(f"  Bundle:       {zip_path}")
        click.echo("═" * 60 + "\n")

    except Exception as exc:
        click.echo(f"✗ Cohort export failed: {exc}", err=True)
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# list-cohorts
# ---------------------------------------------------------------------------

@cli.command("list-cohorts")
@db_options
def list_cohorts_cmd(host, port, dbname, user, password, schema):
    """List all existing cohorts in the OMOP database."""
    from core.cohort_builder import CohortBuilder

    conn = _connect(host, port, dbname, user, password)

    try:
        builder = CohortBuilder(conn, schema=schema)
        cohorts = builder.list_cohorts()

        if not cohorts:
            click.echo("\n  No cohorts found.\n")
            return

        click.echo(f"\n{'ID':>6}  {'Persons':>10}  {'Start':>12}  {'End':>12}")
        click.echo("─" * 50)
        for c in cohorts:
            click.echo(
                f"{c['cohort_definition_id']:>6}  "
                f"{c['person_count']:>10,}  "
                f"{str(c['earliest_start'] or 'N/A'):>12}  "
                f"{str(c['latest_end'] or 'N/A'):>12}"
            )
        click.echo("")

    except Exception as exc:
        click.echo(f"✗ Could not list cohorts: {exc}", err=True)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    cli()
