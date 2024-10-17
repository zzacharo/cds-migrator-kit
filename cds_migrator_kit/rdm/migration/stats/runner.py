# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# Invenio-RDM-Migrator is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""InvenioRDM migration streams runner."""

from pathlib import Path

import yaml
from invenio_rdm_migrator.logging import FailedTxLogger, Logger
from invenio_rdm_migrator.streams import Stream

from cds_migrator_kit.records.log import RDMJsonLogger


# local version of the invenio-rdm-migrator Runner class
# Skipping the default invenio-rdm-migrator StateDB
# most likely it won't be needed
class RecordStatsRunner:
    """ETL streams runner."""

    def _read_config(self, filepath):
        """Read config from file."""
        with open(filepath) as f:
            return yaml.safe_load(f)

    def __init__(self, stream_definition, filepath, config, dry_run):
        """Constructor."""
        self.config = config
        # self.tmp_dir = Path(config.get("tmp_dir"))
        # self.tmp_dir.mkdir(parents=True, exist_ok=True)

        # self.state_dir = Path(config.get("state_dir"))
        # self.state_dir.mkdir(parents=True, exist_ok=True)

        # self.log_dir = Path(config.get("log_dir"))
        # self.log_dir.mkdir(parents=True, exist_ok=True)

        # Logger.initialize(self.log_dir)
        # RDMJsonLogger.initialize(self.log_dir)
        # FailedTxLogger.initialize(self.log_dir)
        # self.db_uri = config.get("db_uri")
        # start parsing streams

        self.stream = Stream(
            stream_definition.name,
            extract=stream_definition.extract_cls(filepath),
            transform=stream_definition.transform_cls(),
            load=stream_definition.load_cls(dry_run=dry_run, config=config),
        )

    def run(self):
        """Run ETL streams."""
        # migration_logger = RDMJsonLogger()
        # migration_logger.start_log()
        try:
            self.stream.run()
        except Exception as e:
            Logger.get_logger().exception(
                f"Stream {self.stream.name} failed.", exc_info=1
            )
            # migration_logger.add_log(e)
            raise e
        finally:
            pass
            # migration_logger.finalise()
