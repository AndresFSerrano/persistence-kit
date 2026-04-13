import subprocess
import sys


def test_root_import_does_not_eagerly_load_optional_capabilities():
    code = (
        "import sys; "
        "import persistence_kit; "
        "print('fastapi' in sys.modules, 'jwt' in sys.modules, 'boto3' in sys.modules)"
    )

    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.strip() == "False False False"
