
# kagimi 
# かぎみ

Scraping and snapshotting property listings.

Quick start

- Activate your virtualenv (example):

```bash
python -m venv venv
source venv/bin/activate
```

- Install dependencies:

```bash
pip install -r requirements.txt
```

Run scripts

- Check available properties (fetch and write outputs):

```bash
python main.py
```

- Check for changes since the last snapshot (alerts):

```bash
python alerts.py
```

Notes

- `main.py` fetches units and writes CSV/SQLite snapshots in `out/`.
- `alerts.py` compares the latest snapshot with the previous one and prints alerts.


Automation

This project can be run via CI (GitHub Actions) to produce daily snapshots; state can be persisted between runs using cached artifacts.

