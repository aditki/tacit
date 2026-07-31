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


def _arbitrary_equality_literal(value: str) -> str | None:
    selector = _selector_value(value)
    if not selector.startswith("==="):
        return None
    literal = selector[3:]
    if not literal:
        return None
    try:
        Version(literal)
    except InvalidVersion:
        return literal
    return None


def _plain_literal(value: str) -> str | None:
    selector = _selector_value(value)
    if not selector or selector.startswith(_SPECIFIER_OPERATORS):
        return None
    return selector


@dataclass(frozen=True)
class _Bound:
    version: Version
    inclusive: bool


@dataclass(frozen=True)
class _WildcardPrefix:
    epoch: int
    release: tuple[int, ...]


def _wildcard_prefix(value: str) -> _WildcardPrefix | None:
    if not value.endswith(".*"):
        return None
    try:
        version = Version(value[:-2])
    except InvalidVersion:
        return None
    return _WildcardPrefix(version.epoch, version.release)


def _prefix_contains(container: _WildcardPrefix, candidate: _WildcardPrefix) -> bool:
    return (
        container.epoch == candidate.epoch
        and len(container.release) <= len(candidate.release)
        and candidate.release[: len(container.release)] == container.release
    )


def _wildcard_constraints_are_disjoint(specifiers: list[Specifier]) -> bool:
    narrowest: _WildcardPrefix | None = None
    for item in specifiers:
        if item.operator != "==" or (prefix := _wildcard_prefix(item.version)) is None:
            continue
        if narrowest is None or _prefix_contains(narrowest, prefix):
            narrowest = prefix
        elif not _prefix_contains(prefix, narrowest):
            return True
    if narrowest is None:
        return False
    exclusions = [
        prefix
        for item in specifiers
        if item.operator == "!=" and (prefix := _wildcard_prefix(item.version)) is not None
    ]
    return any(_prefix_contains(exclusion, narrowest) for exclusion in exclusions)


def _wildcard_exclusions_cover_bounds(
    specifiers: list[Specifier],
    lower: _Bound | None,
    upper: _Bound | None,
) -> bool:
    if lower is None or upper is None:
        return False
    exclusions = sorted(
        (
            bounds
            for item in specifiers
            if item.operator == "!=" and (bounds := _wildcard_bounds(item.version)) is not None
        ),
        key=lambda bounds: bounds[0].version,
    )
    cursor = lower.version
    for exclusion_lower, exclusion_upper in exclusions:
        if exclusion_upper.version <= cursor:
            continue
        if exclusion_lower.version > cursor:
            return False
        cursor = max(cursor, exclusion_upper.version)
        if cursor > upper.version or (cursor == upper.version and not upper.inclusive):
            return True
    return False


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


def _release_version(epoch: int, release: list[int]) -> Version:
    value = ".".join(str(part) for part in release)
    return Version(f"{epoch}!{value}" if epoch else value)


def _compatible_upper(version: Version) -> Version | None:
    release = list(version.release)
    if len(release) < 2:
        return None
    prefix = release[:-1]
    prefix[-1] += 1
    return _release_version(version.epoch, prefix)


def _wildcard_bounds(value: str) -> tuple[_Bound, _Bound] | None:
    if not value.endswith(".*"):
        return None
    try:
        version = Version(value[:-2])
    except InvalidVersion:
        return None
    release = list(version.release)
    if not release:
        return None
    upper = list(release)
    upper[-1] += 1
    return (
        _Bound(_release_version(version.epoch, release), True),
        _Bound(_release_version(version.epoch, upper), False),
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
    if _wildcard_constraints_are_disjoint(combined):
        return False
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
    if _wildcard_exclusions_cover_bounds(combined, lower, upper):
        return False
    return True


def version_selectors_overlap(left: str, right: str) -> bool:
    """Return whether two exact or PEP 440 version selectors can match."""
    left_arbitrary = _arbitrary_equality_literal(left)
    right_arbitrary = _arbitrary_equality_literal(right)
    if left_arbitrary is not None or right_arbitrary is not None:
        if left_arbitrary is not None and right_arbitrary is not None:
            return left_arbitrary == right_arbitrary
        arbitrary = left_arbitrary if left_arbitrary is not None else right_arbitrary
        literal = _plain_literal(right if left_arbitrary is not None else left)
        return literal is not None and arbitrary == literal

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
