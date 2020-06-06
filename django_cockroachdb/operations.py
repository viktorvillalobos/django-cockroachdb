import time
from datetime import timedelta

from django.contrib.gis.db.backends.postgis.operations import (
    PostGISOperations as PostgresDatabaseOperations,
)
from django.db.utils import OperationalError
from psycopg2 import errorcodes
from pytz import timezone


class DatabaseOperations(PostgresDatabaseOperations):
    integer_field_ranges = {
        'SmallIntegerField': (-32768, 32767),
        'IntegerField': (-9223372036854775808, 9223372036854775807),
        'BigIntegerField': (-9223372036854775808, 9223372036854775807),
        'PositiveSmallIntegerField': (0, 32767),
        'PositiveBigIntegerField': (0, 9223372036854775807),
        'PositiveIntegerField': (0, 9223372036854775807),
        'SmallAutoField': (-32768, 32767),
        'AutoField': (-9223372036854775808, 9223372036854775807),
        'BigAutoField': (-9223372036854775808, 9223372036854775807),
    }

    def deferrable_sql(self):
        # Deferrable constraints aren't supported:
        # https://github.com/cockroachdb/cockroach/issues/31632
        return ''

    def adapt_datetimefield_value(self, value):
        """
        Add a timezone to datetimes so that psycopg2 will cast it to
        TIMESTAMPTZ (as cockroach expects) rather than TIMESTAMP.
        """
        # getattr() guards against F() objects which don't have tzinfo.
        if value and getattr(value, 'tzinfo', '') is None and self.connection.timezone_name is not None:
            connection_timezone = timezone(self.connection.timezone_name)
            try:
                value = connection_timezone.localize(value)
            except OverflowError:
                # Localizing datetime.datetime.max (used to cache a value
                # forever, for example) may overflow. Subtract a day to prevent
                # that.
                value -= timedelta(days=1)
                value = connection_timezone.localize(value)
        return value

    def sequence_reset_by_name_sql(self, style, sequences):
        # Not implemented: https://github.com/cockroachdb/cockroach/issues/20956
        return []

    def sequence_reset_sql(self, style, model_list):
        return []

    def explain_query_prefix(self, format=None, **options):
        if format:
            raise ValueError("CockroachDB's EXPLAIN doesn't support any formats.")
        prefix = self.explain_prefix
        extra = [name for name, value in options.items() if value]
        if extra:
            prefix += ' (%s)' % ', '.join(extra)
        return prefix

    def execute_sql_flush(self, sql_list):
        # Retry TRUNCATE if it fails with a serialization error.
        num_retries = 10
        initial_retry_delay = 0.5  # The initial retry delay, in seconds.
        backoff_ = 1.5  # For each retry, the last delay is multiplied by this.
        next_retry_delay = initial_retry_delay
        for retry in range(1, num_retries + 1):
            try:
                return super().execute_sql_flush(sql_list)
            except OperationalError as exc:
                if (getattr(exc.__cause__, 'pgcode', '') != errorcodes.SERIALIZATION_FAILURE or
                        retry >= num_retries):
                    raise
                time.sleep(next_retry_delay)
                next_retry_delay *= backoff_

    def sql_flush(self, style, tables, *, reset_sequences=False, allow_cascade=False):
        # CockroachDB doesn't support resetting sequences.
        return super().sql_flush(style, tables, reset_sequences=False, allow_cascade=allow_cascade)

    def postgis_lib_version(self):
        return '3.0'

    def postgis_version(self):
        "Return PostGIS version number and compile-time options."
        return (2, 5)

    @property
    def gis_operators(self):
        ops = PostgresDatabaseOperations.gis_operators.copy()
        # gis_tests.geoapp.tests.GeoLookupTest.test_strictly_above_below_lookups
        del ops['strictly_above']  # <<|
        del ops['strictly_below']  # |>>
        return ops

    unsupported_functions = {
        'AsGML',  # unknown function: st_asgml(): https://github.com/cockroachdb/cockroach/issues/48877
        'AsKML',  # unknown signature: st_askml(geometry, int): https://github.com/cockroachdb/cockroach/issues/48881
        'AsSVG',  # unknown function: st_assvg(): # https://github.com/cockroachdb/cockroach/issues/48883
        'BoundingCircle',  # unknown function: st_minimumboundingcircle(): https://github.com/cockroachdb/cockroach/issues/48987
        'GeometryDistance',  # <-> operator
        'LineLocatePoint',  # unknown function: st_linelocatepoint(): https://github.com/cockroachdb/cockroach/issues/48973
        'MemSize',  # unknown function: st_memsize(): https://github.com/cockroachdb/cockroach/issues/48985
    }
