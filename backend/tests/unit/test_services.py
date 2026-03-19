"""
tests/unit/test_state.py
"""

import uuid
import pytest
from unittest.mock import patch

from app.models.logs import JobStatus, OperationType
from app.services.migration.state import JobStateService


@pytest.mark.asyncio
async def test_create_job(db_session):
    svc = JobStateService(db_session)
    job_id = await svc.create_job(
        table_name="F_CEL_NETWORK_EVENT",
        operation=OperationType.MIGRATION_PARTITIONED,
        config={"batch_size_days": 1},
    )
    assert isinstance(job_id, uuid.UUID)
    await db_session.commit()


@pytest.mark.asyncio
async def test_job_lifecycle(db_session):
    svc = JobStateService(db_session)

    job_id = await svc.create_job(
        table_name="TEST_TABLE",
        operation=OperationType.MIGRATION_SIMPLE,
    )
    await db_session.commit()

    await svc.start_job(job_id, total_partitions=10)
    await db_session.commit()

    await svc.register_partitions(job_id, ["2024-01-01", "2024-01-02"])
    await db_session.commit()

    await svc.update_partition(job_id, "2024-01-01", JobStatus.DONE, rows_loaded=50_000)
    await db_session.commit()

    progress = await svc.get_progress(job_id)
    assert progress is not None
    assert progress.status == JobStatus.RUNNING
    assert progress.done_partitions == 1

    await svc.finish_job(job_id, total_rows=50_000)
    await db_session.commit()

    progress = await svc.get_progress(job_id)
    assert progress.status == JobStatus.DONE


@pytest.mark.asyncio
async def test_fail_job(db_session):
    svc = JobStateService(db_session)
    job_id = await svc.create_job("ERROR_TABLE", OperationType.VALIDATION)
    await db_session.commit()
    await svc.start_job(job_id)
    await db_session.commit()
    await svc.fail_job(job_id, error="ORA-01555: snapshot too old")
    await db_session.commit()

    progress = await svc.get_progress(job_id)
    assert progress.status == JobStatus.ERROR


# ── XML Parser tests ──────────────────────────────────────────────────────────

"""
tests/unit/test_xml_parser.py
"""
import textwrap
from app.services.datastage.xml_parser import DataStageXMLParser


SAMPLE_DSX = textwrap.dedent("""
    <DSExport>
      <Job Identifier="JOB_F_CEL_NETWORK_EVENT" JobType="Parallel">
        <Stage Identifier="ORA_SOURCE" StageType="OracleConnectorPX">
          <XMLProperties><root>
            <Property Name="TableName" Value="F_CEL_NETWORK_EVENT"/>
            <Property Name="SchemaName" Value="DWADM"/>
            <Property Name="SelectStatement" Value="SELECT * FROM DWADM.F_CEL_NETWORK_EVENT WHERE DT_REFERENCIA = ?"/>
          </root></XMLProperties>
          <OutputPin Identifier="OUTPUT_0">
            <Column Identifier="ID" DataType="int8" Nullable="false"/>
            <Column Identifier="DT_REFERENCIA" DataType="timestamp" Nullable="true"/>
          </OutputPin>
        </Stage>
        <Stage Identifier="SNOW_TARGET" StageType="SnowflakeConnectorPX">
          <XMLProperties><root>
            <Property Name="TableName" Value="F_CEL_NETWORK_EVENT"/>
            <Property Name="SchemaName" Value="DWADM"/>
          </root></XMLProperties>
        </Stage>
        <Link From="ORA_SOURCE" To="SNOW_TARGET"/>
      </Job>
    </DSExport>
""")


def test_parse_job_extracts_names():
    parser = DataStageXMLParser()
    result = parser.parse_content(SAMPLE_DSX)
    assert len(result["jobs"]) == 1
    job = result["jobs"][0]
    assert job["job_name"] == "JOB_F_CEL_NETWORK_EVENT"
    assert "F_CEL_NETWORK_EVENT" in job["source_tables"]
    assert "F_CEL_NETWORK_EVENT" in job["target_tables"]


def test_parse_sql_extraction():
    parser = DataStageXMLParser()
    result = parser.parse_content(SAMPLE_DSX)
    job = result["jobs"][0]
    assert any("SELECT" in q for q in job["sql_queries"] if q)


def test_lineage_graph_structure():
    parser = DataStageXMLParser()
    metadata = parser.parse_content(SAMPLE_DSX)
    graph = parser.build_lineage_graph(metadata)
    assert len(graph.nodes) >= 3  # source + job + target
    assert len(graph.edges) >= 2
    types = {n.type for n in graph.nodes}
    assert "source" in types
    assert "job" in types
    assert "target" in types


def test_partition_list_generation():
    from app.services.migration.partitioned import PartitionedMigrationService
    from app.models.schemas import MigrationRequest
    from app.models.logs import OperationType

    request = MigrationRequest(
        table_name="TEST",
        operation=OperationType.MIGRATION_PARTITIONED,
        date_from="2024-01-01",
        date_to="2024-01-05",
        batch_size_days=1,
    )
    svc = PartitionedMigrationService.__new__(PartitionedMigrationService)
    partitions = svc._build_partition_list(request)
    assert len(partitions) == 4
    assert partitions[0]["key"] == "2024-01-01"
    assert partitions[-1]["key"] == "2024-01-04"
