"""Loading and validation for workflow schemas and disposition catalogues."""

from __future__ import annotations

import csv
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType


SUPPORTED_FIELD_TYPES = frozenset({"string", "phone", "date", "decimal", "enum"})
DEFAULT_WORKFLOW_SCHEMA_FILENAMES = (
    "motor_renewal.json",
    "policy_feedback.json",
)
DEFAULT_DISPOSITION_CATALOGUE_FILENAME = "dispositions.csv"
_CATALOGUE_COLUMNS = frozenset(
    {"workflow_id", "code", "label", "is_terminal", "is_success"}
)
_HEADER_SEPARATOR = re.compile(r"[_\s]+")


class ConfigurationError(ValueError):
    """Raised when an assignment configuration file is missing or invalid."""


@dataclass(frozen=True, slots=True)
class WorkflowField:
    """One field declared by a workflow's lead schema."""

    name: str
    field_type: str
    required: bool
    pii: bool
    reference: bool
    aliases: tuple[str, ...]
    values: tuple[str, ...]
    default: str | None


@dataclass(frozen=True, slots=True)
class WorkflowSchema:
    """A validated workflow schema, preserving the source field order."""

    workflow_id: str
    description: str
    fields: tuple[WorkflowField, ...]

    @property
    def reference_field(self) -> WorkflowField:
        """Return the one field used as the reporting reference."""

        return next(field for field in self.fields if field.reference)

    @property
    def phone_field(self) -> WorkflowField:
        """Return the one field used for lead identity and event matching."""

        return next(field for field in self.fields if field.field_type == "phone")


@dataclass(frozen=True, slots=True)
class Disposition:
    """A workflow-scoped disposition outcome."""

    workflow_id: str
    code: str
    label: str
    is_terminal: bool
    is_success: bool


@dataclass(frozen=True, slots=True)
class LedgerConfiguration:
    """The validated configuration needed by later ingestion and reporting code."""

    workflows: Mapping[str, WorkflowSchema]
    dispositions: Mapping[tuple[str, str], Disposition]

    def disposition_for(self, workflow_id: str, code: str) -> Disposition | None:
        """Return a disposition by its workflow-scoped code, if it exists."""

        return self.dispositions.get((workflow_id, code))


def normalise_header(value: str) -> str:
    """Normalise a schema header using the assignment's matching rules."""

    return _HEADER_SEPARATOR.sub(" ", value.strip().casefold())


def assignment_schema_directory(project_root: str | Path) -> Path:
    """Return the known Assignment A configuration directory for a repository."""

    return Path(project_root) / "assignment-a-data" / "schemas"


def load_assignment_configuration(project_root: str | Path) -> LedgerConfiguration:
    """Load the two Assignment A workflows and their shared catalogue.

    File names are part of the assignment contract. Field definitions and
    disposition values are always read from the supplied configuration files.
    """

    schema_directory = assignment_schema_directory(project_root)
    return load_configuration(
        (
            schema_directory / filename
            for filename in DEFAULT_WORKFLOW_SCHEMA_FILENAMES
        ),
        schema_directory / DEFAULT_DISPOSITION_CATALOGUE_FILENAME,
    )


def load_configuration(
    workflow_schema_paths: Iterable[str | Path],
    disposition_catalogue_path: str | Path,
) -> LedgerConfiguration:
    """Load arbitrary workflow schema files and a disposition catalogue.

    All files are validated before a configuration object is returned. This
    avoids deferring configuration errors until a drop is being processed.
    """

    schema_paths = tuple(Path(path) for path in workflow_schema_paths)
    if not schema_paths:
        raise ConfigurationError("At least one workflow schema path is required.")

    workflows: dict[str, WorkflowSchema] = {}
    for schema_path in schema_paths:
        workflow = load_workflow_schema(schema_path)
        if workflow.workflow_id in workflows:
            raise ConfigurationError(
                f"Duplicate workflow_id {workflow.workflow_id!r} in schema files."
            )
        workflows[workflow.workflow_id] = workflow

    dispositions = load_disposition_catalogue(
        disposition_catalogue_path,
        workflow_ids=workflows,
    )
    return LedgerConfiguration(
        workflows=MappingProxyType(dict(workflows)),
        dispositions=dispositions,
    )


def load_workflow_schema(path: str | Path) -> WorkflowSchema:
    """Load and validate one workflow JSON schema."""

    schema_path = Path(path)
    raw_schema = _load_json_object(schema_path)
    context = f"Workflow schema {schema_path}"

    workflow_id = _nonempty_string(raw_schema.get("workflow_id"), f"{context}.workflow_id")
    description = _nonempty_string(raw_schema.get("description"), f"{context}.description")
    raw_fields = raw_schema.get("fields")
    if not isinstance(raw_fields, list) or not raw_fields:
        raise ConfigurationError(f"{context}.fields must be a non-empty JSON array.")

    fields: list[WorkflowField] = []
    names: set[str] = set()
    for position, raw_field in enumerate(raw_fields, start=1):
        field_context = f"{context}.fields[{position}]"
        if not isinstance(raw_field, dict):
            raise ConfigurationError(f"{field_context} must be a JSON object.")
        field = _parse_field(raw_field, field_context)
        if field.name in names:
            raise ConfigurationError(f"{context} contains duplicate field name {field.name!r}.")
        names.add(field.name)
        fields.append(field)

    _validate_schema_fields(fields, context)
    return WorkflowSchema(
        workflow_id=workflow_id,
        description=description,
        fields=tuple(fields),
    )


def load_disposition_catalogue(
    path: str | Path,
    *,
    workflow_ids: Iterable[str],
) -> Mapping[tuple[str, str], Disposition]:
    """Load a strict, workflow-scoped disposition CSV catalogue."""

    catalogue_path = Path(path)
    allowed_workflow_ids = set(workflow_ids)
    if not catalogue_path.is_file():
        raise ConfigurationError(
            f"Disposition catalogue does not exist or is not a file: {catalogue_path}"
        )

    try:
        with catalogue_path.open("r", encoding="utf-8", newline="") as source:
            reader = csv.DictReader(source)
            headers = reader.fieldnames
            if headers is None:
                raise ConfigurationError(
                    f"Disposition catalogue {catalogue_path} is missing a header row."
                )
            if len(headers) != len(_CATALOGUE_COLUMNS) or set(headers) != _CATALOGUE_COLUMNS:
                expected = ", ".join(sorted(_CATALOGUE_COLUMNS))
                raise ConfigurationError(
                    f"Disposition catalogue {catalogue_path} must have exactly these "
                    f"columns: {expected}."
                )

            dispositions: dict[tuple[str, str], Disposition] = {}
            workflow_counts = dict.fromkeys(allowed_workflow_ids, 0)
            for row_number, row in enumerate(reader, start=2):
                if not any(value and value.strip() for value in row.values()):
                    continue
                context = f"Disposition catalogue {catalogue_path}, row {row_number}"
                workflow_id = _nonempty_string(row.get("workflow_id"), f"{context}.workflow_id")
                code = _nonempty_string(row.get("code"), f"{context}.code")
                label = _nonempty_string(row.get("label"), f"{context}.label")
                is_terminal = _csv_bool(row.get("is_terminal"), f"{context}.is_terminal")
                is_success = _csv_bool(row.get("is_success"), f"{context}.is_success")

                if workflow_id not in allowed_workflow_ids:
                    raise ConfigurationError(
                        f"{context} references unknown workflow_id {workflow_id!r}."
                    )
                key = (workflow_id, code)
                if key in dispositions:
                    raise ConfigurationError(
                        f"{context} duplicates disposition code {code!r} for "
                        f"workflow {workflow_id!r}."
                    )
                dispositions[key] = Disposition(
                    workflow_id=workflow_id,
                    code=code,
                    label=label,
                    is_terminal=is_terminal,
                    is_success=is_success,
                )
                workflow_counts[workflow_id] += 1
    except OSError as error:
        raise ConfigurationError(
            f"Unable to read disposition catalogue {catalogue_path}: {error}"
        ) from error

    missing_catalogues = sorted(
        workflow_id for workflow_id, count in workflow_counts.items() if count == 0
    )
    if missing_catalogues:
        raise ConfigurationError(
            "Disposition catalogue has no rows for workflow(s): "
            + ", ".join(missing_catalogues)
            + "."
        )
    return MappingProxyType(dispositions)


def _load_json_object(path: Path) -> dict[str, object]:
    if not path.is_file():
        raise ConfigurationError(f"Workflow schema does not exist or is not a file: {path}")
    try:
        with path.open("r", encoding="utf-8") as source:
            value = json.load(source)
    except json.JSONDecodeError as error:
        raise ConfigurationError(
            f"Workflow schema {path} is not valid JSON: {error.msg} "
            f"at line {error.lineno}, column {error.colno}."
        ) from error
    except OSError as error:
        raise ConfigurationError(f"Unable to read workflow schema {path}: {error}") from error

    if not isinstance(value, dict):
        raise ConfigurationError(f"Workflow schema {path} must contain a JSON object.")
    return value


def _parse_field(raw_field: dict[str, object], context: str) -> WorkflowField:
    name = _nonempty_string(raw_field.get("name"), f"{context}.name")
    field_type = _nonempty_string(raw_field.get("type"), f"{context}.type")
    if field_type not in SUPPORTED_FIELD_TYPES:
        supported = ", ".join(sorted(SUPPORTED_FIELD_TYPES))
        raise ConfigurationError(
            f"{context}.type {field_type!r} is unsupported; expected one of: {supported}."
        )

    required = _required_bool(raw_field, "required", context)
    pii = _optional_bool(raw_field, "pii", context, default=False)
    reference = _optional_bool(raw_field, "reference", context, default=False)
    aliases = _string_tuple(raw_field.get("aliases", []), f"{context}.aliases")
    values = _string_tuple(raw_field.get("values", []), f"{context}.values")

    default: str | None = None
    if "default" in raw_field:
        default = _nonempty_string(raw_field["default"], f"{context}.default")

    if field_type == "enum":
        if not values:
            raise ConfigurationError(f"{context}.values must be non-empty for an enum field.")
        normalised_values = [value.casefold() for value in values]
        if len(set(normalised_values)) != len(normalised_values):
            raise ConfigurationError(f"{context}.values must not contain duplicate values.")
        if default is not None and default.casefold() not in normalised_values:
            raise ConfigurationError(
                f"{context}.default must be one of the configured enum values."
            )
    elif values:
        raise ConfigurationError(f"{context}.values is only valid for an enum field.")
    elif default is not None:
        raise ConfigurationError(f"{context}.default is only valid for an enum field.")

    return WorkflowField(
        name=name,
        field_type=field_type,
        required=required,
        pii=pii,
        reference=reference,
        aliases=aliases,
        values=values,
        default=default,
    )


def _validate_schema_fields(fields: list[WorkflowField], context: str) -> None:
    reference_fields = [field for field in fields if field.reference]
    if len(reference_fields) != 1:
        raise ConfigurationError(f"{context} must declare exactly one reference field.")

    phone_fields = [field for field in fields if field.field_type == "phone"]
    if len(phone_fields) != 1:
        raise ConfigurationError(f"{context} must declare exactly one phone field.")
    phone_field = phone_fields[0]
    if not phone_field.required or not phone_field.pii:
        raise ConfigurationError(f"{context} phone field must be required and marked as PII.")

    header_owners: dict[str, str] = {}
    for field in fields:
        for header in (field.name, *field.aliases):
            normalised = normalise_header(header)
            if not normalised:
                raise ConfigurationError(
                    f"{context} field {field.name!r} has an empty header or alias."
                )
            previous_owner = header_owners.setdefault(normalised, field.name)
            if previous_owner != field.name:
                raise ConfigurationError(
                    f"{context} has ambiguous header {header!r}, shared by fields "
                    f"{previous_owner!r} and {field.name!r}."
                )


def _nonempty_string(value: object, context: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"{context} must be a non-empty string.")
    return value.strip()


def _required_bool(raw_field: Mapping[str, object], key: str, context: str) -> bool:
    if key not in raw_field:
        raise ConfigurationError(f"{context}.{key} is required and must be a boolean.")
    return _bool(raw_field[key], f"{context}.{key}")


def _optional_bool(
    raw_field: Mapping[str, object], key: str, context: str, *, default: bool
) -> bool:
    if key not in raw_field:
        return default
    return _bool(raw_field[key], f"{context}.{key}")


def _bool(value: object, context: str) -> bool:
    if type(value) is not bool:
        raise ConfigurationError(f"{context} must be a boolean.")
    return value


def _string_tuple(value: object, context: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ConfigurationError(f"{context} must be a JSON array of non-empty strings.")
    return tuple(_nonempty_string(item, f"{context} item") for item in value)


def _csv_bool(value: object, context: str) -> bool:
    if not isinstance(value, str):
        raise ConfigurationError(f"{context} must be 'true' or 'false'.")
    normalised = value.strip().casefold()
    if normalised == "true":
        return True
    if normalised == "false":
        return False
    raise ConfigurationError(f"{context} must be 'true' or 'false'.")
