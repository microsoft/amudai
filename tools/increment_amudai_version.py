#!/usr/bin/env python3
"""
Script to increment or change versions of all amudai crates in the workspace Cargo.toml.

This script modifies the root Cargo.toml file to update versions of:
- The workspace.package version
- All amudai and amudai-* crate dependencies

Usage:
    python increment_amudai_version.py [--current-version <version>] [--new-version <version>]

Arguments:
    --current-version: Verify that all amudai crates currently have this exact version
    --new-version: Set all amudai crates to this version. If not provided, increment patch number
    
Version format: semver (major.minor.patch)
"""

import argparse
import re
import sys
from pathlib import Path
from typing import List, Tuple, Optional


def parse_version(version_str: str) -> Tuple[int, int, int]:
    """Parse a semver version string into (major, minor, patch) tuple."""
    try:
        parts = version_str.split('.')
        if len(parts) != 3:
            raise ValueError(
                f"Version must be in format major.minor.patch, got: {version_str}")
        return (int(parts[0]), int(parts[1]), int(parts[2]))
    except (ValueError, IndexError) as e:
        raise ValueError(f"Invalid version format '{version_str}': {e}")


def format_version(major: int, minor: int, patch: int) -> str:
    """Format version tuple back to string."""
    return f"{major}.{minor}.{patch}"


def increment_patch(version_str: str) -> str:
    """Increment the patch version by 1."""
    major, minor, patch = parse_version(version_str)
    return format_version(major, minor, patch + 1)


def find_amudai_crates(cargo_toml_content: str) -> List[str]:
    """Find all amudai crate names in the Cargo.toml."""
    pattern = r'^(amudai(?:-[a-zA-Z0-9-]+)?) = \{'
    crate_names = []

    for line in cargo_toml_content.split('\n'):
        match = re.match(pattern, line.strip())
        if match:
            crate_names.append(match.group(1))

    return sorted(crate_names)


def get_current_versions(cargo_toml_content: str, crate_names: List[str]) -> dict:
    """Extract current versions of all amudai crates."""
    versions = {}

    # Get workspace package version
    workspace_pattern = r'^\[workspace\.package\]'
    version_pattern = r'^version = "([^"]+)"'

    lines = cargo_toml_content.split('\n')
    in_workspace_package = False

    for line in lines:
        line = line.strip()
        if re.match(workspace_pattern, line):
            in_workspace_package = True
            continue
        elif line.startswith('[') and in_workspace_package:
            in_workspace_package = False
            continue
        elif in_workspace_package:
            match = re.match(version_pattern, line)
            if match:
                versions['workspace.package'] = match.group(1)
                continue

    # Get individual crate versions
    for crate_name in crate_names:
        pattern = rf'^{re.escape(crate_name)} = \{{ version = "([^"]+)"'
        for line in lines:
            match = re.match(pattern, line.strip())
            if match:
                versions[crate_name] = match.group(1)
                break

    return versions


def verify_current_version(versions: dict, expected_version: str) -> bool:
    """Verify that all amudai crates have the expected version."""
    mismatched = []

    for crate_name, current_version in versions.items():
        if current_version != expected_version:
            mismatched.append(f"{crate_name}: {current_version}")

    if mismatched:
        print(
            f"ERROR: The following crates do not have version {expected_version}:")
        for mismatch in mismatched:
            print(f"  {mismatch}")
        return False

    print(f"✓ All amudai crates have version {expected_version}")
    return True


def update_versions(cargo_toml_content: str, crate_names: List[str], new_version: str) -> str:
    """Update all amudai crate versions in the Cargo.toml content."""
    lines = cargo_toml_content.split('\n')
    updated_lines = []
    in_workspace_package = False

    for line in lines:
        original_line = line
        stripped_line = line.strip()

        # Handle workspace package version
        if re.match(r'^\[workspace\.package\]', stripped_line):
            in_workspace_package = True
            updated_lines.append(original_line)
            continue
        elif stripped_line.startswith('[') and in_workspace_package:
            in_workspace_package = False
        elif in_workspace_package and re.match(r'^version = "', stripped_line):
            # Update workspace package version, preserving indentation
            indent = line[:len(line) - len(line.lstrip())]
            updated_lines.append(f'{indent}version = "{new_version}"')
            continue

        # Handle individual crate versions
        updated = False
        for crate_name in crate_names:
            pattern = rf'^({re.escape(crate_name)} = \{{ version = ")[^"]+(".*\}}.*)$'
            match = re.match(pattern, stripped_line)
            if match:
                # Preserve indentation
                indent = line[:len(line) - len(line.lstrip())]
                updated_lines.append(
                    f'{indent}{match.group(1)}{new_version}{match.group(2)}')
                updated = True
                break

        if not updated:
            updated_lines.append(original_line)

    return '\n'.join(updated_lines)


def main():
    parser = argparse.ArgumentParser(
        description="Increment or change versions of all amudai crates",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        '--current-version',
        help='Verify that all amudai crates currently have this exact version'
    )
    parser.add_argument(
        '--new-version',
        help='Set all amudai crates to this version (if not provided, increment patch number)'
    )

    args = parser.parse_args()

    # Find the workspace root Cargo.toml
    script_dir = Path(__file__).parent
    workspace_root = script_dir.parent
    cargo_toml_path = workspace_root / 'Cargo.toml'

    if not cargo_toml_path.exists():
        print(f"ERROR: Could not find Cargo.toml at {cargo_toml_path}")
        sys.exit(1)

    print(f"Reading {cargo_toml_path}")

    # Read the current Cargo.toml
    try:
        with open(cargo_toml_path, 'r', encoding='utf-8') as f:
            cargo_toml_content = f.read()
    except Exception as e:
        print(f"ERROR: Failed to read {cargo_toml_path}: {e}")
        sys.exit(1)

    # Find all amudai crates
    crate_names = find_amudai_crates(cargo_toml_content)
    print(f"Found {len(crate_names)} amudai crates: {', '.join(crate_names)}")

    # Get current versions
    current_versions = get_current_versions(cargo_toml_content, crate_names)
    if not current_versions:
        print("ERROR: Could not find any amudai crate versions")
        sys.exit(1)

    print("Current versions:")
    for crate_name, version in current_versions.items():
        print(f"  {crate_name}: {version}")

    # Verify current version if requested
    if args.current_version:
        try:
            parse_version(args.current_version)  # Validate format
        except ValueError as e:
            print(f"ERROR: {e}")
            sys.exit(1)

        if not verify_current_version(current_versions, args.current_version):
            print(
                f"ERROR: Failed to verify current version match ({current_versions})")
            sys.exit(1)

    # Determine new version
    if args.new_version:
        new_version = args.new_version
        try:
            parse_version(new_version)  # Validate format
        except ValueError as e:
            print(f"ERROR: {e}")
            sys.exit(1)
    else:
        # Get the workspace package version and increment its patch number
        workspace_version = current_versions.get('workspace.package')
        if not workspace_version:
            print("ERROR: Could not find workspace.package version")
            sys.exit(1)

        try:
            new_version = increment_patch(workspace_version)
        except ValueError as e:
            print(f"ERROR: {e}")
            sys.exit(1)

    print(f"Setting all amudai crates to version: {new_version}")

    # Update the Cargo.toml content
    updated_content = update_versions(
        cargo_toml_content, crate_names, new_version)

    # Write the updated content back
    try:
        with open(cargo_toml_path, 'w', encoding='utf-8') as f:
            f.write(updated_content)
        print(f"✓ Successfully updated {cargo_toml_path}")
    except Exception as e:
        print(f"ERROR: Failed to write {cargo_toml_path}: {e}")
        sys.exit(1)

    print("✓ Version update completed successfully!")


if __name__ == '__main__':
    main()
