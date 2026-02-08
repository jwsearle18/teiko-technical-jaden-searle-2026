import polars as pl
from sqlalchemy import String, Integer, ForeignKey, create_engine, Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, Session


class Base(DeclarativeBase):
    pass


class Project(Base):
    __tablename__ = "projects"

    id: Mapped[str] = mapped_column(String, primary_key=True)

    subjects: Mapped[list["Subject"]] = relationship(back_populates="project")

    def __repr__(self) -> str:
        return f"Project(id='{self.id}')"


class Subject(Base):
    __tablename__ = "subjects"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"))
    condition: Mapped[str] = mapped_column(String)
    age: Mapped[int] = mapped_column(Integer)
    sex: Mapped[str] = mapped_column(String)
    treatment: Mapped[str | None] = mapped_column(String, nullable=True)
    response: Mapped[str | None] = mapped_column(String, nullable=True)

    project: Mapped["Project"] = relationship(back_populates="subjects")
    samples: Mapped[list["Sample"]] = relationship(back_populates="subject")

    def __repr__(self) -> str:
        return f"Subject(id='{self.id}', project_id='{self.project_id}')"


class Sample(Base):
    __tablename__ = "samples"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    subject_id: Mapped[str] = mapped_column(ForeignKey("subjects.id"))
    sample_type: Mapped[str] = mapped_column(String)
    time_from_treatment_start: Mapped[int] = mapped_column(Integer)

    subject: Mapped["Subject"] = relationship(back_populates="samples")
    cell_counts: Mapped[list["CellCount"]] = relationship(back_populates="sample")

    def __repr__(self) -> str:
        return f"Sample(id='{self.id}', subject_id='{self.subject_id}')"


class CellCount(Base):
    __tablename__ = "cell_counts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sample_id: Mapped[str] = mapped_column(ForeignKey("samples.id"))
    population: Mapped[str] = mapped_column(String)
    count: Mapped[int] = mapped_column(Integer)

    sample: Mapped["Sample"] = relationship(back_populates="cell_counts")

    def __repr__(self) -> str:
        return f"CellCount(sample_id='{self.sample_id}', population='{self.population}', count={self.count})"


CELL_POPULATIONS = ["b_cell", "cd8_t_cell", "cd4_t_cell", "nk_cell", "monocyte"]


def init_db(db_path: str) -> Engine:
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    return engine


def load_csv(engine: Engine, csv_path: str) -> None:
    df = pl.read_csv(csv_path)

    with Session(engine) as session:
        projects = df.select("project").unique()
        for row in projects.iter_rows(named=True):
            session.add(Project(id=row["project"]))

        subjects = df.select(
            "subject", "project", "condition", "age", "sex", "treatment", "response"
        ).unique()
        for row in subjects.iter_rows(named=True):
            session.add(Subject(
                id=row["subject"],
                project_id=row["project"],
                condition=row["condition"],
                age=row["age"],
                sex=row["sex"],
                treatment=row["treatment"],
                response=row["response"],
            ))

        samples = df.select(
            "sample", "subject", "sample_type", "time_from_treatment_start"
        ).unique()
        for row in samples.iter_rows(named=True):
            session.add(Sample(
                id=row["sample"],
                subject_id=row["subject"],
                sample_type=row["sample_type"],
                time_from_treatment_start=row["time_from_treatment_start"],
            ))

        cell_counts = df.select("sample", *CELL_POPULATIONS).unpivot(
            on=CELL_POPULATIONS,
            index="sample",
            variable_name="population",
            value_name="count",
        )
        for row in cell_counts.iter_rows(named=True):
            session.add(CellCount(
                sample_id=row["sample"],
                population=row["population"],
                count=row["count"],
            ))

        session.commit()
