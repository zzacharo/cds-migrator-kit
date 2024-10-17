# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration extract module."""

import json
from os import listdir
from os.path import isfile, join
from pathlib import Path

import click
from invenio_rdm_migrator.extract import Extract


class LegacyRecordStatsExtract(Extract):
    """LegacyRecordStatsExtract."""

    EVENT_TYPES = ["events.pageviews", "events.downloads"]
    LEGACY_INDICES = [
        "cds-2004",
        "cds-2005",
        "cds-2006",
        "cds-2007",
        "cds-2008",
        "cds-2009",
        "cds-2010",
        "cds-2011",
        "cds-2012",
        "cds-2013",
        "cds-2014",
        "cds-2015",
        "cds-2016",
        "cds-2017",
        "cds-2018",
        "cds-2019",
        "cds-2020",
        "cds-2021",
        "cds-2022",
        "cds-2023",
        "cds-2024",
    ]

    def __init__(self, filepath, **kwargs):
        """Constructor."""
        self.filepath = Path(filepath).absolute()

    def run(self):
        """Run."""
        with open(self.filepath, "r") as dump_file:
            data = json.load(dump_file)
            with click.progressbar(data) as records:
                for dump_record in records:
                    for index_name in self.LEGACY_INDICES:
                        for t in self.EVENT_TYPES:
                            yield (index_name, t, dump_record)
