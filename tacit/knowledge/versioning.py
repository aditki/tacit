"""Version selector matching for Operational Knowledge scopes."""

from __future__ import annotations

from dataclasses import dataclass

from packaging.specifiers import InvalidSpecifier, Specifier, SpecifierSet
from packaging.version import InvalidVersion, Version

_VERSION_PREFIX = "version:"
_SPECIFIER_OPERATORS = ("===", "~=", "==", "!=", "<=", ">=", "<", ">")


def _selector_value(value: str) -> str:
    normalized = str(value).strip().casefold()
    return normalized.removeprefix(_VERSION_PREFIX).strip()


def _specifier_set(value: str) -> SpecifierSet | None:
    selector = _selector_value(value)
    if not selector.startswith(_SPECIFIER_OPERATORS):
        return None
    try:
        return SpecifierSet(selector)
    except InvalidSpecifier:
        return None


def _version(value: str) -> Version | None:
    selector = _selector_value(value)
    if selector.startswith(_SPECIFIER_OPERATORS):
        return None
    try:
        return Version(selector)
    except InvalidVersion:
        return None


def _contains(specifiers: SpecifierSet, version: Version) -> bool:
    return specifiers.contains(version, prereleases=True)


@dataclass(frozen=True)
class _Bound:
    version: Version
    inclusive: bool


def _stronger_lower(current: _Bound | None, candidate: _Bound) -> _Bound:
    if current is None or candidate.version > current.version:
        return candidate
    if candidate.version == current.version and not candidate.inclusive:
        return candidate
    return current


def _stronger_upper(current: _Bound | None, candidate: _Bound) -> _Bound:
    if current is None or candidate.version < current.version:
        return candidate
    if candidate.version == current.version and not candidate.inclusive:
        return candidate
    return current


def _compatible_upper(version: Version) -> Version | None:
    release = list(version.release)
    if len(release) < 2:
        return None
    prefix = release[:-1]
    prefix[-1] += 1
    return Version(".".join(str(part) for part in prefix))


def _wildcard_bounds(value: str) -> tuple[_Bound, _Bound] | None:
    if not value.endswith(".*"):
        return None
    try:
        release = list(Version(value[:-2]).release)
    except InvalidVersion:
        return None
    if not release:
        return None
    upper = list(release)
    upper[-1] += 1
    return (
        _Bound(Version(".".join(str(part) for part in release)), True),
        _Bound(Version(".".join(str(part) for part in upper)), False),
    )


def _bounds(specifiers: list[Specifier]) -> tuple[_Bound | None, _Bound | None]:
    lower: _Bound | None = None
    upper: _Bound | None = None
    for item in specifiers:
        operator = item.operator
        value = item.version
        try:
            version = Version(value.removesuffix(".*"))
        except InvalidVersion:
            continue
        if operator in {">", ">="}:
            lower = _stronger_lower(lower, _Bound(version, operator == ">="))
        elif operator in {"<", "<="}:
            upper = _stronger_upper(upper, _Bound(version, operator == "<="))
        elif operator == "~=":
            lower = _stronger_lower(lower, _Bound(version, True))
            compatible_upper = _compatible_upper(version)
            if compatible_upper is not None:
                upper = _stronger_upper(upper, _Bound(compatible_upper, False))
        elif operator == "==" and value.endswith(".*"):
            wildcard = _wildcard_bounds(value)
            if wildcard is not None:
                lower = _stronger_lower(lower, wildcard[0])
                upper = _stronger_upper(upper, wildcard[1])
    return lower, upper


def _exact_versions(specifiers: list[Specifier]) -> set[Version]:
    exact: set[Version] = set()
    for item in specifiers:
        if item.operator not in {"==", "==="} or item.version.endswith(".*"):
            continue
        try:
            exact.add(Version(item.version))
        except InvalidVersion:
            continue
    return exact


def _specifier_sets_overlap(left: SpecifierSet, right: SpecifierSet) -> bool:
    combined = [*left, *right]
    exact_versions = _exact_versions(combined)
    if exact_versions:
        return any(_contains(left, version) and _contains(right, version) for version in exact_versions)

    lower, upper = _bounds(combined)
    if lower is None or upper is None:
        return True
    if lower.version > upper.version:
        return False
    if lower.version == upper.version:
        return (
            lower.inclusive and upper.inclusive and _contains(left, lower.version) and _contains(right, lower.version)
        )
    # A non-empty open interval has representable PEP 440 versions. Exclusion
    # specifiers may remove points, but cannot exhaust an interval unless an
    # explicit exact/prefix constraint above narrows it first.
    return True


def version_selectors_overlap(left: str, right: str) -> bool:
    """Return whether two exact or PEP 440 version selectors can match."""
    left_specifiers = _specifier_set(left)
    right_specifiers = _specifier_set(right)
    left_version = _version(left)
    right_version = _version(right)

    if left_specifiers is not None and right_version is not None:
        return _contains(left_specifiers, right_version)
    if right_specifiers is not None and left_version is not None:
        return _contains(right_specifiers, left_version)
    if left_specifiers is not None and right_specifiers is not None:
        return _specifier_sets_overlap(left_specifiers, right_specifiers)
    if left_version is not None and right_version is not None:
        return left_version == right_version
    return _selector_value(left) == _selector_value(right)


def version_scope_applies(required: list[str] | set[str], actual: list[str] | set[str]) -> bool:
    """Match a governed version scope against concrete investigation versions."""
    if not required:
        return True
    if not actual:
        return False
    return any(version_selectors_overlap(expected, observed) for expected in required for observed in actual)


def version_scopes_overlap(left: list[str] | set[str], right: list[str] | set[str]) -> bool:
    """Return whether two knowledge scopes may cover at least one common version."""
    if not left or not right:
        return True
    return any(version_selectors_overlap(left_value, right_value) for left_value in left for right_value in right)
