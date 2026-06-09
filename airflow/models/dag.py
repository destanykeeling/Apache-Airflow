from datetime import datetime
import logging
from sqlalchemy.exc import IntegrityError, OperationalError, InterfaceError
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

class DAG:
    def __init__(self, dag_id: str):
        self.dag_id = dag_id
        self.log = logger

    def create_dagrun(
        self,
        state,
        execution_date: datetime | None = None,
        run_id: str | None = None,
        start_date: datetime | None = None,
        external_trigger: bool | None = None,
        conf: dict | None = None,
        creating_job_id: int | None = None,
        session: Session = None,
        data_interval = None,
    ):
        from airflow.models.dagrun import DagRun
        
        run = DagRun(
            dag_id=self.dag_id,
            run_id=run_id,
            execution_date=execution_date,
            start_date=start_date,
            external_trigger=external_trigger,
            conf=conf,
            state=state,
            creating_job_id=creating_job_id,
            data_interval=data_interval,
        )
        try:
            session.add(run)
            session.commit()
        except IntegrityError:
            self.log.warning(
                "Failed to create DagRun %s because it already exists. "
                "Rolling back and retrieving existing DagRun.",
                run_id,
            )
            session.rollback()
            filters = [DagRun.dag_id == self.dag_id]
            if run_id and execution_date:
                filters.append((DagRun.run_id == run_id) | (DagRun.execution_date == execution_date))
            elif run_id:
                filters.append(DagRun.run_id == run_id)
            elif execution_date:
                filters.append(DagRun.execution_date == execution_date)
            
            run = session.query(DagRun).filter(*filters).one()
        except (OperationalError, InterfaceError):
            self.log.error("Database error during DagRun creation. Rolling back session.")
            session.rollback()
            raise

        return run