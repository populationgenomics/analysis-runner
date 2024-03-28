from deprecated import deprecated

# 2024-03-22: mfranklin: I checked for code uses, and these are the 3 I found in our org


@deprecated(reason="Use cpg_utils.dataproc.setup_dataproc instead")
def setup_dataproc(*args, **kwargs):  # noqa: ANN002, ANN003
    from cpg_utils.dataproc import setup_dataproc as _setup_dataproc

    return _setup_dataproc(*args, **kwargs)


@deprecated(reason="Use cpg_utils.dataproc.hail_dataproc_job instead")
def hail_dataproc_job(*args, **kwargs):  # noqa: ANN002, ANN003
    from cpg_utils.dataproc import hail_dataproc_job as _hail_dataproc_job

    return _hail_dataproc_job(*args, **kwargs)


@deprecated(reason="Use cpg_utils.dataproc._add_submit_job instead")
def _add_submit_job(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
    from cpg_utils.dataproc import _add_submit_job as __add_submit_job

    return __add_submit_job(*args, **kwargs)
