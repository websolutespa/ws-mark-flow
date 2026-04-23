#!/usr/bin/env python3
"""
Check for newer versions of packages in requirements.txt and update the file.
Does NOT install packages - only updates version pins in requirements.txt.
Preserves comments, blank lines, and packages without version specifiers.
"""

import re
import subprocess
import sys
from pathlib import Path


def parse_requirement_line(line):
    """
    Parse a requirement line and extract package name, version, and extras.
    Returns: (package_base, version, extras, inline_comment, is_versioned)
    """
    stripped = line.strip()

    # Empty line or pure comment
    if not stripped or stripped.startswith('#'):
        return None, None, None, None, False

    # Split inline comment
    if '#' in stripped:
        req_part, comment = stripped.split('#', 1)
        inline_comment = '#' + comment
        req_part = req_part.strip()
    else:
        req_part = stripped
        inline_comment = None

    # Check if it has a version specifier
    has_version = '==' in req_part

    # Extract package name and extras
    match = re.match(r'^([a-zA-Z0-9_-]+)(\[.*?\])?', req_part)
    if not match:
        return None, None, None, inline_comment, False

    package_base = match.group(1)
    extras = match.group(2) or ''

    # Extract version if present
    version = None
    if has_version:
        version_match = re.search(r'==([0-9.]+)', req_part)
        if version_match:
            version = version_match.group(1)

    return package_base, version, extras, inline_comment, has_version


def get_latest_version(package_name):
    """Get the latest version of a package from PyPI."""
    try:
        import urllib.request
        import json
        
        # Query PyPI JSON API directly
        url = f"https://pypi.org/pypi/{package_name}/json"
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode())
            return data['info']['version']
            
    except Exception as e:
        # Fallback: try using pip index
        try:
            result = subprocess.run(
                ['python', '-m', 'pip', 'index', 'versions', package_name],
                capture_output=True,
                text=True,
                check=True,
                timeout=10
            )
            
            # Parse output - look for "Available versions:" line
            lines = result.stdout.split('\n')
            for i, line in enumerate(lines):
                if 'Available versions:' in line.lower() or package_name.lower() in line.lower():
                    # Next line or same line might contain versions
                    version_match = re.search(r'(\d+\.\d+\.\d+(?:\.\d+)?)', lines[i] if i < len(lines) else '')
                    if not version_match and i + 1 < len(lines):
                        version_match = re.search(r'(\d+\.\d+\.\d+(?:\.\d+)?)', lines[i + 1])
                    if version_match:
                        return version_match.group(1)
            
            # If still not found, try the first version number in output
            for line in lines:
                version_match = re.search(r'(\d+\.\d+\.\d+(?:\.\d+)?)', line)
                if version_match:
                    return version_match.group(1)
                    
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
            pass
    
    return None


def compare_versions(current, latest):
    """
    Compare two version strings.
    Returns: True if latest > current, False otherwise
    """
    try:
        current_parts = [int(x) for x in current.split('.')]
        latest_parts = [int(x) for x in latest.split('.')]
        
        # Pad shorter version with zeros
        max_len = max(len(current_parts), len(latest_parts))
        current_parts += [0] * (max_len - len(current_parts))
        latest_parts += [0] * (max_len - len(latest_parts))
        
        return latest_parts > current_parts
    except (ValueError, AttributeError):
        return False


def update_requirements(requirements_path='requirements.txt', dry_run=False):
    """Main function to check and update requirements.txt file."""
    req_file = Path(requirements_path)

    if not req_file.exists():
        print(f"Error: {requirements_path} not found", file=sys.stderr)
        sys.exit(1)

    # Read original file
    with open(req_file, 'r') as f:
        lines = f.readlines()

    print(f"Checking for package updates in {requirements_path}...\n")

    updated_lines = []
    updates_available = []
    no_updates = []
    errors = []

    # Process each line
    for line in lines:
        package_base, old_version, extras, inline_comment, is_versioned = parse_requirement_line(line)

        if package_base and is_versioned:
            print(f"Checking {package_base}...", end=' ', flush=True)
            
            latest_version = get_latest_version(package_base)
            
            if latest_version is None:
                print(f"⚠ Could not fetch latest version")
                errors.append(package_base)
                updated_lines.append(line)  # Keep original
            elif compare_versions(old_version, latest_version):
                print(f"✓ Update available: {old_version} → {latest_version}")
                updates_available.append((package_base, old_version, latest_version))
                
                # Reconstruct the line with new version
                new_line = f"{package_base}{extras}=={latest_version}"
                if inline_comment:
                    new_line += f" {inline_comment}"
                new_line += '\n'
                updated_lines.append(new_line)
            else:
                print(f"✓ Up to date ({old_version})")
                no_updates.append(package_base)
                updated_lines.append(line)  # Keep original
        else:
            # Keep non-versioned lines, comments, and blank lines as-is
            updated_lines.append(line)

    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    if updates_available:
        print(f"\n✓ Updates available for {len(updates_available)} package(s):")
        for pkg, old, new in updates_available:
            print(f"  • {pkg}: {old} → {new}")
    
    if no_updates:
        print(f"\n✓ Already up to date: {len(no_updates)} package(s)")
    
    if errors:
        print(f"\n⚠ Could not check: {len(errors)} package(s)")
        for pkg in errors:
            print(f"  • {pkg}")

    # Write updated file (unless dry run)
    if updates_available:
        if dry_run:
            print(f"\n[DRY RUN] Would update {requirements_path}")
            print("Run without --dry-run to apply changes")
        else:
            with open(req_file, 'w') as f:
                f.writelines(updated_lines)
            print(f"\n✓ Successfully updated {requirements_path}")
    else:
        print(f"\nℹ No updates needed for {requirements_path}")

    return len(updates_available)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Check for newer package versions and update requirements.txt'
    )
    parser.add_argument(
        'requirements_file',
        nargs='?',
        default='requirements.txt',
        help='Path to requirements.txt file (default: requirements.txt)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Check for updates without modifying the file'
    )
    
    args = parser.parse_args()
    
    update_requirements(args.requirements_file, args.dry_run)
