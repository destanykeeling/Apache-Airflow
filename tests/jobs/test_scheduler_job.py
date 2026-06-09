import unittest
from unittest.mock import MagicMock
from sqlalchemy.exc import IntegrityError, OperationalError
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models.dag import DAG

class TestSchedulerJob(unittest.TestCase):
    def test_create_dag_runs_handles_database_failover(self):
        session = MagicMock()
        
        dag = MagicMock(spec=DAG)
        dag.dag_id = "test_dag"
        
        dag_model = MagicMock()
        dag_model.dag_id = "test_dag"
        dag_model.dag = dag
        dag_model.execution_date = "2023-01-01"
        dag_model.run_id = "test_run"
        
        dag.create_dagrun.side_effect = [
            OperationalError("statement", {}, "failover"),
            IntegrityError("statement", {}, "already exists"),
        ]
        
        runner = SchedulerJobRunner(job=MagicMock())
        
        with self.assertRaises(OperationalError):
            runner._create_dag_runs([dag_model], session)
        
        session.rollback.assert_called_once()
        session.rollback.reset_mock()
        
        runner._create_dag_runs([dag_model], session)
        
        session.rollback.assert_called_once()