# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Circulation Items CLI."""
import glob
import json
import logging
import os

import click
from flask import current_app
from flask.cli import with_appcontext

from invenio_accounts.models import User
from invenio_db import db
from invenio_userprofiles.models import UserProfile
from invenio_oauthclient.models import UserIdentity, RemoteAccount

from cds_migrator_kit.circulation.users.api import UserMigrator

logger = logging.getLogger(__name__)

@with_appcontext
def users(users_json):
    """Load users from JSON files and output ILS Records."""
    total_import_records = 0
    total_migrated_records = 0

    click.secho(users_json, fg='green')
    with open(users_json, 'r') as fp:
        users = json.load(fp)
        total_import_records = len(users)

    users, users_profiles, users_identities, remote_accounts = UserMigrator(users).migrate()

    total_migrated_records = len(users)

    import_users(users, users_identities, users_profiles, remote_accounts)

    _log = "Total number of migrated records: {0}/{1}".format(
        total_migrated_records, total_import_records)
    logger.info(_log)


def import_users(users, users_identities, users_profiles, remote_accounts):

    click.secho(users, fg='green')
    with open(users, 'r') as fp:
        users = json.load(fp)
        for user in users:
            user = User(**user)
            db.session.add(user)

    click.secho(users_identities, fg='green')
    with open(users_identities, 'r') as fp:
        users = json.load(fp)
        for user in users:
            user = UserIdentity(**user)
            db.session.add(user)

    click.secho(users_profiles, fg='green')
    with open(users_profiles, 'r') as fp:
        users = json.load(fp)
        for user in users:
            user = UserProfile(**user)
            db.session.add(user)

    click.secho(remote_accounts, fg='green')
    with open(remote_accounts, 'r') as fp:
        users = json.load(fp)
        for user in users:
            user = RemoteAccount(client_id='zzacharo_ils', **user)
            db.session.add(user)

    db.session.commit()