#  Copyright 2025 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
MotherDuck connection unit tests
"""

from metadata.generated.schema.entity.services.connections.database.motherDuckConnection import (
    MotherDuckConnection,
)
from metadata.ingestion.source.database.motherduck.connection import (
    _get_connect_args,
    get_connection_url,
)


def _make_connection(
    token: str = "test-token-123", database: str = None
) -> MotherDuckConnection:
    return MotherDuckConnection(token=token, database=database)


class TestGetConnectionUrl:
    def test_url_with_database(self):
        conn = _make_connection(database="my_db")
        assert get_connection_url(conn) == "duckdb:///md:my_db"

    def test_url_without_database(self):
        conn = _make_connection(database=None)
        assert get_connection_url(conn) == "duckdb:///md:"

    def test_token_not_in_url(self):
        conn = _make_connection(token="super-secret", database="prod")
        assert "super-secret" not in get_connection_url(conn)


class TestGetConnectArgs:
    def test_token_present(self):
        conn = _make_connection(token="my-service-token")
        args = _get_connect_args(conn)
        assert args["motherduck_token"] == "my-service-token"

    def test_only_motherduck_token_key(self):
        conn = _make_connection(token="abc")
        args = _get_connect_args(conn)
        assert set(args.keys()) == {"motherduck_token"}
