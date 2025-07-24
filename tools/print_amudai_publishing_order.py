#!/usr/bin/env python3
"""
Script to generate topologically sorted order for publishing amudai crates.

This script uses 'cargo tree' to analyze dependencies between amudai crates and generates 
a publishing order that ensures dependencies are published before their dependents.

Usage:
    python print_amudai_publishing_order.py [--output-format FORMAT] [--verbose]

Arguments:
    --output-format: Output format. Options: 'bash' (default), 'yaml', 'list'
    --verbose: Show detailed dependency analysis
    
The script uses cargo tree to get accurate dependency information from Cargo itself.

Example usage:
    # Generate bash script format for pipeline (default)
    python print_amudai_publishing_order.py
    
    # Generate with detailed analysis
    python print_amudai_publishing_order.py --verbose
    
    # Generate as YAML list
    python print_amudai_publishing_order.py --output-format yaml
    
    # Generate as simple list
    python print_amudai_publishing_order.py --output-format list

To update the pipeline:
    1. Run: python print_amudai_publishing_order.py
    2. Copy the output and replace the corresponding section in .pipelines/OneBranch.Official.yml
    3. The output will be in the correct format to replace lines 88-114 in the pipeline file
"""

import argparse
import heapq
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict


class DependencyAnalyzer:
    """Analyzes dependencies between amudai crates using cargo tree to determine publishing order."""
    
    def __init__(self, workspace_root: Path, verbose: bool = False):
        self.workspace_root = workspace_root
        self.verbose = verbose
        self.crates: Dict[str, str] = {}  # crate_name -> relative_path
        self.dependencies: Dict[str, Set[str]] = defaultdict(set)
        self.amudai_crates: Set[str] = set()
        
    def find_amudai_crates(self) -> None:
        """Find all amudai crates using cargo metadata."""
        try:
            # Get workspace metadata
            result = subprocess.run(
                ['cargo', 'metadata', '--format-version', '1', '--no-deps'],
                cwd=self.workspace_root,
                capture_output=True,
                text=True,
                check=True
            )
            metadata = json.loads(result.stdout)
            
            workspace_members = set(metadata.get('workspace_members', []))
            
            for package in metadata.get('packages', []):
                package_id = package['id']
                package_name = package['name']
                
                # Only include workspace members that start with 'amudai'
                if package_id in workspace_members and package_name.startswith('amudai'):
                    # Get relative path from workspace root to crate
                    manifest_path = Path(package['manifest_path'])
                    crate_dir = manifest_path.parent
                    relative_path = crate_dir.relative_to(self.workspace_root / 'rust')
                    
                    self.crates[package_name] = str(relative_path).replace('\\', '/')
                    self.amudai_crates.add(package_name)
            
            if self.verbose:
                print(f"Found {len(self.amudai_crates)} amudai crates:")
                for crate in sorted(self.amudai_crates):
                    print(f"  {crate} -> {self.crates[crate]}")
                print()
                
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to run cargo metadata: {e.stderr}")
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse cargo metadata JSON: {e}")
    
    def analyze_dependencies_with_cargo_tree(self) -> None:
        """Analyze dependencies using cargo tree for each amudai crate."""
        for crate_name in self.amudai_crates:
            try:
                # Run cargo tree for this specific crate with depth 1 to get direct dependencies only
                result = subprocess.run(
                    ['cargo', 'tree', '-p', crate_name, '--depth', '1', '--format', '{p}'],
                    cwd=self.workspace_root,
                    capture_output=True,
                    text=True,
                    check=True
                )
                
                lines = result.stdout.strip().split('\n')
                if len(lines) < 2:  # First line is the crate itself
                    continue
                
                # Skip the first line (the crate itself) and parse dependencies
                for line in lines[1:]:
                    line = line.strip()
                    if not line or '[dev-dependencies]' in line or '[build-dependencies]' in line:
                        continue
                    
                    # Remove tree formatting characters using a more effective approach
                    # Extract everything after the first letter found
                    match = re.search(r'([a-zA-Z][a-zA-Z0-9_-]*)', line)
                    if match:
                        dep_name = match.group(1)
                        if dep_name in self.amudai_crates:
                            self.dependencies[crate_name].add(dep_name)
                
                if self.verbose:
                    print(f"Found {len(self.dependencies[crate_name])} dependencies for {crate_name}")
                
            except subprocess.CalledProcessError as e:
                # Some crates might not have dependencies, which is fine
                if self.verbose:
                    print(f"Warning: Could not get dependencies for {crate_name}: {e.stderr}")
                continue
        
        if self.verbose:
            print("Dependencies found:")
            for crate, deps in sorted(self.dependencies.items()):
                if deps:
                    print(f"  {crate} depends on: {', '.join(sorted(deps))}")
                else:
                    print(f"  {crate} has no amudai dependencies")
            print()
    
    def topological_sort(self) -> List[str]:
        """Perform topological sort to determine publishing order with stable alphabetical ordering."""
        # Calculate in-degrees (number of dependencies)
        in_degree = {crate: 0 for crate in self.amudai_crates}
        
        for crate, deps in self.dependencies.items():
            for dep in deps:
                if dep in in_degree:  # Only count amudai dependencies
                    in_degree[crate] += 1
        
        # Use Kahn's algorithm for topological sorting with stable alphabetical ordering
        # Use a min-heap to ensure alphabetical processing of nodes with same in-degree
        heap = [crate for crate, degree in in_degree.items() if degree == 0]
        heapq.heapify(heap)
        result = []
        
        while heap:
            current = heapq.heappop(heap)
            result.append(current)
            
            # Find all crates that depend on the current crate
            dependents_to_add = []
            for crate, deps in self.dependencies.items():
                if current in deps:
                    in_degree[crate] -= 1
                    if in_degree[crate] == 0:
                        dependents_to_add.append(crate)
            
            # Add dependents in alphabetical order to maintain stability
            for dependent in sorted(dependents_to_add):
                heapq.heappush(heap, dependent)
        
        # Check for circular dependencies
        if len(result) != len(self.amudai_crates):
            remaining = self.amudai_crates - set(result)
            raise ValueError(f"Circular dependency detected among crates: {', '.join(sorted(remaining))}")
        
        # Separate support crates and non-support crates while maintaining topological order
        support_crates = []
        non_support_crates = []
        
        for crate in result:
            crate_path = self.get_crate_path_for_publishing(crate)
            if crate_path.startswith('support_crates/'):
                support_crates.append(crate)
            elif crate_path.startswith('tools/'):  # Exclude all tools crates
                continue
            else:
                non_support_crates.append(crate)
        
        # Return support crates first, then non-support crates
        return support_crates + non_support_crates
    
    def get_crate_path_for_publishing(self, crate_name: str) -> str:
        """Get the relative path for a crate as used in publishing scripts."""
        return self.crates[crate_name]


def format_output(sorted_crates: List[str], analyzer: DependencyAnalyzer, output_format: str) -> str:
    """Format the output in the requested format."""
    if output_format == 'bash':
        # Format for bash script like in the pipeline
        paths = [analyzer.get_crate_path_for_publishing(crate) for crate in sorted_crates]
        
        lines = ['for crate in \\']
        for i, path in enumerate(paths):
            if i == len(paths) - 1:
                lines.append(f'                            {path}; do')
            else:
                lines.append(f'                            {path} \\')
        
        return '\n'.join(lines)
    
    elif output_format == 'yaml':
        # Format as YAML list
        paths = [analyzer.get_crate_path_for_publishing(crate) for crate in sorted_crates]
        lines = ['crates:']
        for path in paths:
            lines.append(f'  - {path}')
        return '\n'.join(lines)
    
    elif output_format == 'list':
        # Simple list format
        return '\n'.join(sorted_crates)
    
    else:
        raise ValueError(f"Unknown output format: {output_format}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate topologically sorted order for publishing amudai crates",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        '--output-format',
        choices=['bash', 'yaml', 'list'],
        default='bash',
        help='Output format (default: bash)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Show detailed dependency analysis'
    )

    args = parser.parse_args()

    # Find the workspace root
    script_dir = Path(__file__).parent
    workspace_root = script_dir.parent

    try:
        analyzer = DependencyAnalyzer(workspace_root, args.verbose)
        
        if args.verbose:
            print("Analyzing amudai crate dependencies...")
            print(f"Workspace root: {workspace_root}")
            print()
        
        analyzer.find_amudai_crates()
        analyzer.analyze_dependencies_with_cargo_tree()
        sorted_crates = analyzer.topological_sort()
        
        if args.verbose:
            print("Topologically sorted publishing order:")
            for i, crate in enumerate(sorted_crates, 1):
                deps = analyzer.dependencies.get(crate, set())
                if deps:
                    print(f"  {i:2d}. {crate} (depends on: {', '.join(sorted(deps))})")
                else:
                    print(f"  {i:2d}. {crate} (no amudai dependencies)")
            print()
            print("Output:")
            print("-" * 40)
        
        output = format_output(sorted_crates, analyzer, args.output_format)
        print(output)
        
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
