import subprocess
import sys
import os
import shutil
import argparse

# ANSI escape codes work natively on Linux.
# This check ensures we only run the "enable" command on Windows.
if os.name == 'nt':
    os.system('')

class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def run_command(command, cwd=None, shell=True):
    """Runs a shell command and checks for errors."""
    print(f"{Colors.OKBLUE}shell> {Colors.ENDC}{command}")
    try:
        subprocess.check_call(command, cwd=cwd, shell=shell)
    except subprocess.CalledProcessError as e:
        print(f"{Colors.FAIL}Error running command: {command}{Colors.ENDC}")
        sys.exit(1)

def find_repo_root():
    """Finds the repository root (two levels up from this script)."""
    # This script is in test/samples/, so repo root is two levels up
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
    
    # Verify we found the correct root by checking for Cargo.toml
    if not os.path.exists(os.path.join(repo_root, "Cargo.toml")):
        raise Exception(f"Could not find repository root (Cargo.toml not found at {repo_root})")
    
    return repo_root

def main():
    parser = argparse.ArgumentParser(description="Run Amudai demo ingestion.")
    parser.add_argument("--rebuild", action="store_true", help="Force rebuild of the binaries")
    args = parser.parse_args()

    # Configuration
    repo_root = find_repo_root()
    data_gen_script = os.path.join(repo_root, "test", "samples", "inventory_generate_csv.py")
    schema_file = os.path.join(repo_root, "test", "samples", "inventory_schema.json")
    
    output_dir = os.path.join(repo_root, "demo_output")
    csv_file = os.path.join(output_dir, "data.csv")
    shard_path = os.path.join(output_dir, "my_shard")
    
    # Use cargo run to handle build and execution, avoiding issues with custom target directories
    amudai_cmd = "cargo run -p amudai-cmd --release --"

    # 1. Build (if requested)
    if args.rebuild:
        print(f"\n{Colors.HEADER}--- Building amudai-cmd ---{Colors.ENDC}")
        run_command("cargo build -p amudai-cmd --release", cwd=repo_root)

    # 2. Clean up previous run
    if os.path.exists(output_dir):
        print(f"{Colors.WARNING}Cleaning up {output_dir}...{Colors.ENDC}")
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    # 3. Generate Data
    print(f"\n{Colors.HEADER}--- Generating Data ---{Colors.ENDC}")
    record_count = 10000
    run_command(f"{sys.executable} {data_gen_script} {record_count} {csv_file}")

    # 4. Ingest Data
    print(f"\n{Colors.HEADER}--- Ingesting Data ---{Colors.ENDC}")
    run_command(f"{amudai_cmd} ingest --file {csv_file} --schema-file {schema_file} {shard_path}")

    # 5. Show Head (Content)
    print(f"\n{Colors.HEADER}--- Shard Content (Head) ---{Colors.ENDC}")
    run_command(f"{amudai_cmd} head {shard_path}")

    # 6. Show Summary (Inspect)
    print(f"\n{Colors.HEADER}--- Shard Summary ---{Colors.ENDC}")
    if os.name == 'nt':
        run_command(f"{amudai_cmd} inspect {shard_path} | findstr \"total_record_count\"")
    else:
        run_command(f"{amudai_cmd} inspect {shard_path} | grep \"total_record_count\"")

if __name__ == "__main__":
    main()
