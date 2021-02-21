from contextlib import contextmanager

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import ConnectionState
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import DECIMALS
from dbt.adapters.spark import __version__

try:
    from TCLIService.ttypes import TOperationState as ThriftState
    from thrift.transport import THttpClient
    from pyhive import hive
except ImportError:
    ThriftState = None
    THttpClient = None
    hive = None
try:
    import pyodbc
except ImportError:
    pyodbc = None
from datetime import datetime
import sqlparams

from hologram.helpers import StrEnum
from dataclasses import dataclass
from typing import Optional

import base64
import time

from msal import ConfidentialClientApplication

NUMBERS = DECIMALS + (int, float)


def _build_odbc_connnection_string(**kwargs) -> str:
    return ";".join([f"{k}={v}" for k, v in kwargs.items()])


class SparkConnectionMethod(StrEnum):
    THRIFT = 'thrift'
    HTTP = 'http'
    ODBC = 'odbc'

class DatabricksAuthenticationMethod(StrEnum):
    TOKEN = 'token'
    AZUREOAUTH = 'azureOAuth'

@dataclass
class SparkCredentials(Credentials):
    host: str
    method: SparkConnectionMethod
    schema: str
    database: Optional[str]
    driver: Optional[str] = None
    cluster: Optional[str] = None
    endpoint: Optional[str] = None
    authMethod: DatabricksAuthenticationMethod = DatabricksAuthenticationMethod.TOKEN
    token: Optional[str] = None
    oauth_client_id: Optional[str] = None
    oauth_client_secret: Optional[str] = None
    oauth_client_authority: Optional[str] = None
    user: Optional[str] = None
    port: int = 443
    auth: Optional[str] = None
    kerberos_service_name: Optional[str] = None
    organization: str = '0'
    connect_retries: int = 0
    connect_timeout: int = 10

    def __post_init__(self):
        # spark classifies database and schema as the same thing
        if (
            self.database is not None and
            self.database != self.schema
        ):
            raise dbt.exceptions.RuntimeException(
                f'    schema: {self.schema} \n'
                f'    database: {self.database} \n'
                f'On Spark, database must be omitted or have the same value as'
                f' schema.'
            )
        self.database = None

        if self.method == SparkConnectionMethod.ODBC and pyodbc is None:
            raise dbt.exceptions.RuntimeException(
                f"{self.method} connection method requires "
                "additional dependencies. \n"
                "Install the additional required dependencies with "
                "`pip install dbt-spark[ODBC]`"
            )

        if (
            self.method == SparkConnectionMethod.ODBC and
            self.cluster and
            self.endpoint
        ):
            raise dbt.exceptions.RuntimeException(
                "`cluster` and `endpoint` cannot both be set when"
                f" using {self.method} method to connect to Spark"
            )

        if (
            self.method == SparkConnectionMethod.HTTP or
            self.method == SparkConnectionMethod.THRIFT
        ) and not (
            ThriftState and THttpClient and hive
        ):
            raise dbt.exceptions.RuntimeException(
                f"{self.method} connection method requires "
                "additional dependencies. \n"
                "Install the additional required dependencies with "
                "`pip install dbt-spark[PyHive]`"
            )
    
    def _get_azure_ad_access_token(self) -> str:
        """   https://msal-python.readthedocs.io/en/latest/#msal.ConfidentialClientApplication  """
        app = ConfidentialClientApplication(
            client_id=self.oauth_client_id, 
            client_credential=self.oauth_client_secret,
            authority=self.oauth_client_authority)

        result = None

        config = {
            "scope": ["https://graph.microsoft.com/.default"],
        }

        if not result:
            result = app.acquire_token_for_client(scopes=config["scope"])

        if "access_token" not in result:
            print(result.get("error"))
            print(result.get("error_description"))
            print(result.get("correlation_id"))  # You might need this when reporting a bug.
            raise dbt.exceptions.FailedToConnectException(
                        'failed to connect to Azure'
                    )
        # print("ACCESS TOKEN:"+ result["access_token"])
        # return result["access_token"]
        return "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Im5PbzNaRHJPRFhFSzFqS1doWHNsSFJfS1hFZyIsImtpZCI6Im5PbzNaRHJPRFhFSzFqS1doWHNsSFJfS1hFZyJ9.eyJhdWQiOiIyZmY4MTRhNi0zMzA0LTRhYjgtODVjYi1jZDBlNmY4NzljMWQiLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC8yY2M0ZjZiZS1kMjMzLTQ5NjgtYTkyYy03NGNlMmQyNmFkYjEvIiwiaWF0IjoxNjEzOTEzNTk2LCJuYmYiOjE2MTM5MTM1OTYsImV4cCI6MTYxMzkxNzQ5NiwiYWNyIjoiMSIsImFpbyI6IkFVUUF1LzhUQUFBQWtlbWNtcGpGdEw2NzNEakJPOWtpdHg2eFZiTDVRalNqTExLaGtJa2hMUUE1Q3c0WVNIb0haWG5nc0FGbVdLeDJaV2JucTZkWFc1M2dhZnZRMnhxS2NnPT0iLCJhbHRzZWNpZCI6IjE6bGl2ZS5jb206MDAwM0JGRkQxM0FBQkU3RiIsImFtciI6WyJwd2QiXSwiYXBwaWQiOiI0NTRjZjY3OS1jOGMzLTQ0OTUtOTllMy1jZWMzMjAxNDU4MDkiLCJhcHBpZGFjciI6IjAiLCJlbWFpbCI6InZhbGRhc0BtYWtzaW1hdmljaXVzLmV1IiwiZmFtaWx5X25hbWUiOiIyZGRhMWYzYS0zMjExLTRjMjAtYTdjMC1jYzliMmY3ZmI4NzkiLCJnaXZlbl9uYW1lIjoiZjk1ZWQ5NzgtY2I0My00OThjLWIxNmUtMTU0MGQ5NWYzNDQxIiwiaWRwIjoibGl2ZS5jb20iLCJpcGFkZHIiOiI3OC41OC4yMzcuMTY2IiwibmFtZSI6ImY5NWVkOTc4LWNiNDMtNDk4Yy1iMTZlLTE1NDBkOTVmMzQ0MSAyZGRhMWYzYS0zMjExLTRjMjAtYTdjMC1jYzliMmY3ZmI4NzkiLCJvaWQiOiIwYjRmNWQ3OC03ZTBlLTQyMzItYmI2MS01NjNmZmVlNDlhNzQiLCJwdWlkIjoiMTAwMzAwMDBBQzAwN0U3NyIsInJoIjoiMC5BVEVBdnZiRUxEUFNhRW1wTEhUT0xTYXRzWG4yVEVYRHlKVkVtZVBPd3lBVVdBa3hBSm8uIiwic2NwIjoidXNlcl9pbXBlcnNvbmF0aW9uIiwic3ViIjoiVGNpaS16bXhDUTkxSjdCWVoxdGpKdkVSb0xLQ0FGQ2NPeGYtYmo3QzF3SSIsInRpZCI6IjJjYzRmNmJlLWQyMzMtNDk2OC1hOTJjLTc0Y2UyZDI2YWRiMSIsInVuaXF1ZV9uYW1lIjoibGl2ZS5jb20jdmFsZGFzQG1ha3NpbWF2aWNpdXMuZXUiLCJ1dGkiOiJNZ1JhczNXUVUwQ3R0d0I3cUVCQkFBIiwidmVyIjoiMS4wIn0.PGjS4bHZTMCFQnbBNUUg1w5G9YTem1dfjM3eiBf1-sVPRRyRyUlB28nB7PjYB_GnR235ZmaBkc3TF0mToGUqC43Zky2a0CWlAEmRyzUH85y8RIWzgnEx5cRi7SC46klzqHa78xnQWoihjPCG23VT8bo3nwKH0JziH6DFBmsrFc0wwqMBm4l_xjf_3MgPr5Fk-GzQ_LtryUvxdiATXAjqiK6Zcmm1fPKZZAdizP4EQImfGALbDwjoIdyggyInnZzOGWJjODXPaFMpbNnKM8LWGcpMgNfCeShfWchOSn5JqNvVBMIrsRkuVzo8eDOK_e63YTASncj4ncNRJ4EDtlBQGA"

    def get_odbc_auth_args(self):
        result = {}

        if self.authMethod == DatabricksAuthenticationMethod.TOKEN:
            result["UID"] = "token"    
            result["PWD"] = self.token
            result["AuthMech"] = 3

        elif self.authMethod == DatabricksAuthenticationMethod.AZUREOAUTH: 
            result["AuthMech"] = 11
            result["Auth_Flow"] = 0
            result["Auth_AccessToken"] = self._get_azure_ad_access_token()   

        else:
            raise dbt.exceptions.DbtProfileError(
                f"invalid credential method: {self.authMethod}"
            )
    
        return result

    @property
    def type(self):
        return 'spark'

    def _connection_keys(self):
        return ('host', 'port', 'cluster',
                'endpoint', 'schema', 'organization')


class PyhiveConnectionWrapper(object):
    """Wrap a Spark connection in a way that no-ops transactions"""
    # https://forums.databricks.com/questions/2157/in-apache-spark-sql-can-we-roll-back-the-transacti.html  # noqa

    def __init__(self, handle):
        self.handle = handle
        self._cursor = None

    def cursor(self):
        self._cursor = self.handle.cursor()
        return self

    def cancel(self):
        if self._cursor:
            # Handle bad response in the pyhive lib when
            # the connection is cancelled
            try:
                self._cursor.cancel()
            except EnvironmentError as exc:
                logger.debug(
                    "Exception while cancelling query: {}".format(exc)
                )

    def close(self):
        if self._cursor:
            # Handle bad response in the pyhive lib when
            # the connection is cancelled
            try:
                self._cursor.close()
            except EnvironmentError as exc:
                logger.debug(
                    "Exception while closing cursor: {}".format(exc)
                )

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    def fetchall(self):
        return self._cursor.fetchall()

    def execute(self, sql, bindings=None):
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        # Reaching into the private enumeration here is bad form,
        # but there doesn't appear to be any way to determine that
        # a query has completed executing from the pyhive public API.
        # We need to use an async query + poll here, otherwise our
        # request may be dropped after ~5 minutes by the thrift server
        STATE_PENDING = [
            ThriftState.INITIALIZED_STATE,
            ThriftState.RUNNING_STATE,
            ThriftState.PENDING_STATE,
        ]

        STATE_SUCCESS = [
            ThriftState.FINISHED_STATE,
        ]

        if bindings is not None:
            bindings = [self._fix_binding(binding) for binding in bindings]

        self._cursor.execute(sql, bindings, async_=True)
        poll_state = self._cursor.poll()
        state = poll_state.operationState

        while state in STATE_PENDING:
            logger.debug("Poll status: {}, sleeping".format(state))

            poll_state = self._cursor.poll()
            state = poll_state.operationState

        # If an errorMessage is present, then raise a database exception
        # with that exact message. If no errorMessage is present, the
        # query did not necessarily succeed: check the state against the
        # known successful states, raising an error if the query did not
        # complete in a known good state. This can happen when queries are
        # cancelled, for instance. The errorMessage will be None, but the
        # state of the query will be "cancelled". By raising an exception
        # here, we prevent dbt from showing a status of OK when the query
        # has in fact failed.
        if poll_state.errorMessage:
            logger.debug("Poll response: {}".format(poll_state))
            logger.debug("Poll status: {}".format(state))
            dbt.exceptions.raise_database_error(poll_state.errorMessage)

        elif state not in STATE_SUCCESS:
            status_type = ThriftState._VALUES_TO_NAMES.get(
                state,
                'Unknown<{!r}>'.format(state))

            dbt.exceptions.raise_database_error(
                "Query failed with status: {}".format(status_type))

        logger.debug("Poll status: {}, query complete".format(state))

    @classmethod
    def _fix_binding(cls, value):
        """Convert complex datatypes to primitives that can be loaded by
           the Spark driver"""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        else:
            return value

    @property
    def description(self):
        return self._cursor.description


class PyodbcConnectionWrapper(PyhiveConnectionWrapper):

    def execute(self, sql, bindings=None):
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]
        # pyodbc does not handle a None type binding!
        if bindings is None:
            self._cursor.execute(sql)
        else:
            # pyodbc only supports `qmark` sql params!
            query = sqlparams.SQLParams('format', 'qmark')
            sql, bindings = query.format(sql, bindings)
            self._cursor.execute(sql, *bindings)


class SparkConnectionManager(SQLConnectionManager):
    TYPE = 'spark'

    SPARK_CLUSTER_HTTP_PATH = "/sql/protocolv1/o/{organization}/{cluster}"
    SPARK_SQL_ENDPOINT_HTTP_PATH = "/sql/1.0/endpoints/{endpoint}"
    SPARK_CONNECTION_URL = (
        "https://{host}:{port}" + SPARK_CLUSTER_HTTP_PATH
    )

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except Exception as exc:
            logger.debug("Error while running:\n{}".format(sql))
            logger.debug(exc)
            if len(exc.args) == 0:
                raise

            thrift_resp = exc.args[0]
            if hasattr(thrift_resp, 'status'):
                msg = thrift_resp.status.errorMessage
                raise dbt.exceptions.RuntimeException(msg)
            else:
                raise dbt.exceptions.RuntimeException(str(exc))

    def cancel(self, connection):
        connection.handle.cancel()

    @classmethod
    def get_response(cls, cursor):
        return 'OK'

    # No transactions on Spark....
    def add_begin_query(self, *args, **kwargs):
        logger.debug("NotImplemented: add_begin_query")

    def add_commit_query(self, *args, **kwargs):
        logger.debug("NotImplemented: add_commit_query")

    def commit(self, *args, **kwargs):
        logger.debug("NotImplemented: commit")

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    @classmethod
    def validate_creds(cls, creds, required):
        method = creds.method

        for key in required:
            if not hasattr(creds, key):
                raise dbt.exceptions.DbtProfileError(
                    "The config '{}' is required when using the {} method"
                    " to connect to Spark".format(key, method))

    @classmethod
    def get_spark_cluster_path(cls, creds):
        http_path = None
        if creds.cluster is not None:
            required_fields = ['driver', 'host', 'port', 
                                'organization', 'cluster']
            http_path = cls.SPARK_CLUSTER_HTTP_PATH.format(
                organization=creds.organization,
                cluster=creds.cluster
            )
        elif creds.endpoint is not None:
            required_fields = ['driver', 'host', 'port', 
                                'endpoint']
            http_path = cls.SPARK_SQL_ENDPOINT_HTTP_PATH.format(
                endpoint=creds.endpoint
            )
        else:
            raise dbt.exceptions.DbtProfileError(
                "Either `cluster` or `endpoint` must set when"
                " using the odbc method to connect to Spark"
            )

        cls.validate_creds(creds, required_fields)

        return http_path
    

    @classmethod
    def open(cls, connection):
        if connection.state == ConnectionState.OPEN:
            logger.debug('Connection is already open, skipping open.')
            return connection

        creds = connection.credentials
        exc = None

        for i in range(1 + creds.connect_retries):
            try:
                if creds.method == SparkConnectionMethod.HTTP:
                    cls.validate_creds(creds, ['token', 'host', 'port',
                                               'cluster', 'organization'])

                    conn_url = cls.SPARK_CONNECTION_URL.format(
                        host=creds.host,
                        port=creds.port,
                        organization=creds.organization,
                        cluster=creds.cluster
                    )

                    logger.debug("connection url: {}".format(conn_url))

                    transport = THttpClient.THttpClient(conn_url)

                    raw_token = "token:{}".format(creds.token).encode()
                    token = base64.standard_b64encode(raw_token).decode()
                    transport.setCustomHeaders({
                        'Authorization': 'Basic {}'.format(token)
                    })

                    conn = hive.connect(thrift_transport=transport)
                    handle = PyhiveConnectionWrapper(conn)
                elif creds.method == SparkConnectionMethod.THRIFT:
                    cls.validate_creds(creds,
                                       ['host', 'port', 'user', 'schema'])

                    conn = hive.connect(host=creds.host,
                                        port=creds.port,
                                        username=creds.user,
                                        auth=creds.auth,
                                        kerberos_service_name=creds.kerberos_service_name)  # noqa
                    handle = PyhiveConnectionWrapper(conn)
                elif (creds.method == SparkConnectionMethod.ODBC):
                    http_path = cls.get_spark_cluster_path(creds)

                    dbt_spark_version = __version__.version
                    user_agent_entry = f"fishtown-analytics-dbt-spark/{dbt_spark_version} (Databricks)"  # noqa

                    # https://www.simba.com/products/Spark/doc/v2/ODBC_InstallGuide/unix/content/odbc/options/driver.htm
                    connection_str = _build_odbc_connnection_string(
                        DRIVER=creds.driver,
                        HOST=creds.host,
                        PORT=creds.port,
                        HTTPPath=http_path,
                        SparkServerType=3,
                        ThriftTransport=2,
                        SSL=1,
                        UserAgentEntry=user_agent_entry,
                        **creds.get_odbc_auth_args()
                    )

                    conn = pyodbc.connect(connection_str, autocommit=True)
                    handle = PyodbcConnectionWrapper(conn)
                
                else:
                    raise dbt.exceptions.DbtProfileError(
                        f"invalid credential method: {creds.method}"
                    )
                break
            except Exception as e:
                print("EXCEPTION: {0}".format(e))
                exc = e
                if isinstance(e, EOFError):
                    # The user almost certainly has invalid credentials.
                    # Perhaps a token expired, or something
                    msg = 'Failed to connect'
                    if creds.token is not None:
                        msg += ', is your token valid?'
                    raise dbt.exceptions.FailedToConnectException(msg) from e
                retryable_message = _is_retryable_error(e)
                if retryable_message and creds.connect_retries > 0:
                    msg = (
                        f"Warning: {retryable_message}\n\tRetrying in "
                        f"{creds.connect_timeout} seconds "
                        f"({i} of {creds.connect_retries})"
                    )
                    logger.warning(msg)
                    time.sleep(creds.connect_timeout)
                else:
                    raise dbt.exceptions.FailedToConnectException(
                        'failed to connect'
                    ) from e
        else:
            raise exc

        connection.handle = handle
        connection.state = ConnectionState.OPEN
        return connection

def _is_retryable_error(exc: Exception) -> Optional[str]:
    message = getattr(exc, 'message', None)
    if message is None:
        return None
    message = message.lower()
    if 'pending' in message:
        return exc.message
    if 'temporarily_unavailable' in message:
        return exc.message
    return None
