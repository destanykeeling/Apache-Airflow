import logging
from sqlalchemy.exc import IntegrityError, OperationalError, InterfaceError
from sqlalchemy.orm import Session
from airflow.utils.state import DagRunState

logger = logging.getLogger(__name__)

class SchedulerJobRunner:
    def __init__(self, job, subdir=None, num_runs=-1, num_times_parse_dags=None):
        self.job = job
        self.log = logger

    def _create_dag_runs(self, dag_models, session: Session) -> None:
        for dag_model in dag_models:
            dag = dag_model.dag
            try:
                dag.create_dagrun(
                    state=DagRunState.QUEUED,
                    execution_date=dag_model.execution_date,
                    run_id=dag_model.run_id,
                    session=session,
                )
            except IntegrityError:
                self.log.warning(
                    "DagRun already exists for dag %s, rolling back session.",
                    dag_model.dag_id,
                )
                session.rollback()
            except (OperationalError, InterfaceError):
                self.log.error(
                    "Database error during DagRun creation for dag %s, rolling back session.",
                    dag_model.dag_id,
                )
                session.rollback()
                raise