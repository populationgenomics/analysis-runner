"""Shared helpers for analysis-runner resources."""

import pulumi

# TODO, do be removed later


def protected(
    *,
    ignore_changes: list[str] | None = None,
    depends_on: list[pulumi.Resource] | None = None,
) -> pulumi.ResourceOptions:

    return pulumi.ResourceOptions(
        protect=True, ignore_changes=ignore_changes, depends_on=depends_on
    )
