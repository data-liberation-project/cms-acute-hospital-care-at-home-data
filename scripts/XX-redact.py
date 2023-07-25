import re
from pathlib import Path

raw_paths = Path("data/raw/").glob("*.csv")
for path in raw_paths:
    with open(path) as f:
        redacted = re.sub(r"AHCAH-\d{2,}", "AHCAH-***", f.read())
        dest = "data/redacted/" + path.name
        with open(dest, "w") as f:
            f.write(redacted)
