from pathlib import Path
from dotenv import load_dotenv
import databento as db

load_dotenv()

DATASET = "GLBX.MDP3"
SYMBOL = "ES.v.0"
STYPE_IN = "continuous"
SCHEMA = "mbp-1"
START = "2026-07-02T00:00:00"
END = "2026-07-03T00:00:00"

out_dir = Path("data/raw/databento/glbx_mdp3/es_v0/2026-07-02")
out_dir.mkdir(parents=True, exist_ok=True)

out_path = out_dir / "es_v0_2026-07-02_mbp1.dbn.zst"

client = db.Historical()

print("Downloading:")
print(f"  dataset:  {DATASET}")
print(f"  symbol:   {SYMBOL}")
print(f"  schema:   {SCHEMA}")
print(f"  start:    {START}")
print(f"  end:      {END}")
print(f"  out_path: {out_path}")

data = client.timeseries.get_range(
    dataset=DATASET,
    symbols=SYMBOL,
    stype_in=STYPE_IN,
    schema=SCHEMA,
    start=START,
    end=END,
)

data.to_file(out_path)

size_bytes = out_path.stat().st_size
size_mb = size_bytes / (1024 ** 2)
size_gb = size_bytes / (1024 ** 3)

print()
print("Saved:")
print(f"  {out_path}")
print(f"  file size MB: {size_mb:.2f}")
print(f"  file size GB: {size_gb:.3f}")

print()
print("Symbology:")
print(data.symbology)
