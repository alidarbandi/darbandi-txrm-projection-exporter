# Contributing

Thank you for improving Darbandi TXRM Projection Exporter.

## Development Setup

```powershell
python -m pip install -r requirements.txt
```

Run checks before submitting changes:

```powershell
python -m py_compile txrm_read.py
python txrm_read.py --self-test
```

## Data Policy

Do not commit microscopy datasets or generated extraction outputs. The `.gitignore`
file excludes TXRM/TXM files, TIFFs, metadata exports, logs, and preprocessing
outputs by default.

## Bug Reports

When reporting a metadata parsing issue, include:

- the summary file from `metadata/tigre_geometry/`
- relevant warning messages
- a small sanitized metadata excerpt if possible

Do not share private scan data unless you have permission to do so.
