import os
import pandas as pd

BASE = r"output"


def latest_run():
    if not os.path.exists(BASE):
        return None
    return max(
        (os.path.join(BASE, d) for d in os.listdir(BASE) if os.path.isdir(os.path.join(BASE, d))),
        key=os.path.getmtime
    )

def second_latest_run():
    runs = [
        os.path.join('output', d) for d in os.listdir('output') if os.path.isdir(os.path.join('output',d))
    ]

    if len(runs) < 2:
        return False
    
    runs_sorted = sorted(runs, key=os.path.getmtime)
    return runs_sorted[-2]

def load(csv):
    return pd.read_csv(csv)


def main():
    run = latest_run()
    print("Checking RI in:", run)

    # Load hubs
    hubs = {}
    for f in os.listdir(run):
        if f.startswith("Hub_"):
            df = load(os.path.join(run, f))
            hk_col = [c for c in df.columns if "Hash Key" in c][0]
            hubs[f.replace(".csv", "")] = set(df[hk_col])

    errors = 0

    # Check all links
    for f in os.listdir(run):
        if f.startswith("Link_"):
            df = load(os.path.join(run, f))
            for col in df.columns:
                if col.endswith("Hash Key") and not col.startswith(f.replace("Link_", "").replace("_", " ")):
                    # FK column
                    hub = "Hub_" + col.replace(" Hash Key", "").replace(" ", "_")
                    if hub in hubs:
                        missing = set(df[col]) - hubs[hub]
                        if missing:
                            print(f"❌ {f}.{col} -> {hub} missing {len(missing)} keys")
                            errors += 1

    print("\n====================")
    if errors == 0:
        print("🎯 REFERENTIAL INTEGRITY OK")
    else:
        print(f"❗ RI errors: {errors}")
    print("====================")


if __name__ == "__main__":
    main()
