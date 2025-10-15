#!/usr/bin/env -S uv run
# /// script
# dependencies = ["datamodel-code-generator"]
# ///
"""Generate Pydantic models from LamaPoll OpenAPI spec.

Usage:
    uv run scripts/generate_api_models.py
"""

import subprocess
import tempfile
from pathlib import Path

OPENAPI_URL = "https://app.lamapoll.de/assets/api/v2/openapi.json"
OUTPUT_FILE = (
    Path(__file__).parent.parent / "src" / "plumberlama" / "generated_api_models.py"
)


def main():
    print(f"Downloading OpenAPI spec from {OPENAPI_URL}...")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        openapi_file = tmpdir / "openapi.json"

        # Download OpenAPI spec
        subprocess.run(
            ["curl", "-s", OPENAPI_URL, "-o", str(openapi_file)],
            check=True,
        )
        print("Downloaded OpenAPI spec")

        # Generate models directly to output file
        print(f"Generating Pydantic models to {OUTPUT_FILE}...")
        subprocess.run(
            [
                "datamodel-codegen",
                "--input",
                str(openapi_file),
                "--output",
                str(OUTPUT_FILE),
                "--input-file-type",
                "openapi",
                "--field-constraints",
                "--use-standard-collections",
                "--output-model-type",
                "pydantic_v2.BaseModel",
            ],
            check=True,
        )
        print("Generated models successfully!")
        print(f"File size: {OUTPUT_FILE.stat().st_size} bytes")


if __name__ == "__main__":
    main()
