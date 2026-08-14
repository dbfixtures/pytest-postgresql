"""pytest-postgresql's exceptions."""

from typing import Iterable


class ExecutableMissingException(FileNotFoundError):
    """Exception raised when pg_ctl, needed to start a PostgreSQL server, was not found."""

    _REMEDIES = (
        "To fix this, either:\n"
        "  - install the PostgreSQL server package "
        "(apt install postgresql, dnf install postgresql-server, brew install postgresql),\n"
        "  - point the plugin at an existing pg_ctl with the --postgresql-exec command line option, "
        "the postgresql_exec ini option, or the executable factory argument,\n"
        "  - or use the postgresql_noproc fixture to connect to a server you run yourself, "
        "a dockerised one for instance."
    )

    @classmethod
    def _no_pg_ctl(cls, cause: str, checked: Iterable[str]) -> "ExecutableMissingException":
        """Report the locations that were probed, why they came up empty, and what to do."""
        locations = "\n".join(f"  - {location}" for location in checked)
        return cls(
            f"Could not find pg_ctl, which is needed to start a PostgreSQL server.\n"
            f"{cause}\n"
            f"Checked:\n{locations}\n"
            f"{cls._REMEDIES}"
        )

    @classmethod
    def pg_config_unusable(cls, checked: Iterable[str]) -> "ExecutableMissingException":
        """pg_config is missing, not executable, hanging or failing, so it cannot be asked."""
        return cls._no_pg_ctl(
            "pg_config could not be run either, so it could not be used to locate the PostgreSQL binaries.",
            checked,
        )

    @classmethod
    def not_in_bindir(cls, bindir: str, checked: Iterable[str]) -> "ExecutableMissingException":
        """pg_config named a binaries directory, but no pg_ctl lives there."""
        return cls._no_pg_ctl(
            f"pg_config reports PostgreSQL binaries live in {bindir}, but there's no pg_ctl there. "
            f"That usually means only the PostgreSQL client libraries are installed "
            f"(Debian/Ubuntu's libpq-dev, for example), without the matching server package.",
            checked,
        )


class PostgreSQLUnsupported(Exception):
    """Exception raised when unsupported postgresql would be detected."""
