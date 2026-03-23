__all__ = ["build_analytical_model", "run_etl"]


def __getattr__(name: str):
    if name in ("build_analytical_model", "run_etl"):
        from . import pipeline

        return getattr(pipeline, name)
    raise AttributeError(name)
