# Darbandi TXRM Projection Exporter

A desktop and command-line tool for extracting ZEISS Xradia TXRM/TXM projection data, previewing projections, running flat-field and attenuation preprocessing, and scanning reconstruction geometry for TIGRE FDK workflows.

The app preserves native image order and pixel values during extraction. Preview contrast adjustments affect only the GUI display.

## Features

- Extract TXRM/TXM OLE2 image streams to TIFF.
- Export metadata to Excel, CSV, JSON, and logs.
- Preview extracted, flat-field corrected, and attenuation projections.
- Apply flat-field correction from reference images.
- Convert flat-field images to attenuation using `p = -ln(I_corrected)`.
- Parse geometry from `metadata.xlsx` into TIGRE-ready JSON and CSV files.
- Decode difficult Zeiss metadata fields, including mojibake units and binary float payloads.
- Preserve raw cleaned and decoded metadata for auditing.

## Requirements

- Windows, macOS, or Linux with Python 3.10 or newer.
- Tkinter, usually included with standard Python installers.
- Python packages listed in `requirements.txt`.

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

## Launch The GUI

```powershell
python txrm_read.py
```

Basic workflow:

1. Click `Browse` next to `TXRM file`.
2. Choose an output folder, or leave it blank to create one beside the input file.
3. Click `Extract`.
4. Use the `Source` dropdown in Projection Preview to switch between extracted projections, flat-field corrected images, and attenuation images.
5. Click `Flat-field Correct`, `Attenuation`, or `Scan Geometry` after extraction as needed.

## Command-Line Usage

Extract a TXRM/TXM file:

```powershell
python txrm_read.py --input "scan.txrm" --output "scan_export"
```

Scan an existing metadata workbook for TIGRE geometry:

```powershell
python txrm_read.py --metadata "scan_export\metadata\metadata.xlsx" --geometry-output "scan_export\metadata\tigre_geometry"
```

Run lightweight internal checks:

```powershell
python txrm_read.py --self-test
```

## Output Layout

After extraction, the output folder contains:

- `projections/proj_000001.tif`, `proj_000002.tif`, ... in ZEISS `Image1`, `Image2`, ... order.
- `reference/`, `dark_field/`, and `auxiliary_images/` for non-projection image streams.
- `metadata/metadata.xlsx` with metadata, projection geometry, image streams, and OLE streams.
- `metadata/projection_geometry.csv` with angle, position, shift, exposure, current, voltage, size, and dtype columns.
- `metadata/coordinate_system_notes.json` with export coordinate notes.
- `logs/txrm_export.log` with extraction details.

Preprocessing adds:

- `flatfield_corrected/flatfield_000001.tif`, ...
- `flatfield_corrected/reference_average_float32.tif`
- `attenuation/attenuation_000001.tif`, ...

Geometry scanning adds:

- `metadata/tigre_geometry/tigre_fdk_geometry.json`
- `metadata/tigre_geometry/geometry.json`
- `metadata/tigre_geometry/tigre_fdk_geometry_summary.txt`
- `metadata/tigre_geometry/geometry_summary.txt`
- `metadata/tigre_geometry/projection_angle_table.csv`
- `metadata/tigre_geometry/cleaned_metadata_dump.json`

## TIGRE Geometry Notes

The geometry scanner reads `metadata/metadata.xlsx` and promotes reconstruction-relevant values into `scan_parameters`, `distances`, `pixel_size`, and `tigre_geometry`.

Important formulas:

```text
DSO_mm = abs(StoRADistance)
DOD_mm = abs(DtoRADistance)
DSD_mm = DSO_mm + DOD_mm
dDetector_mm = effective_pixel_size_mm * DSD_mm / DSO_mm
```

`DtoRADistance` is treated as detector-to-object / detector-to-rotation-axis distance. The app does not use `DtoRADistance` alone as `DSD`.

The scanner also preserves raw cleaned metadata and decoding notes for odd Zeiss characters, mojibake units such as `um`, and byte-like numeric fields.

## Documentation

See [docs/USER_MANUAL.md](docs/USER_MANUAL.md) for a step-by-step user guide.

## Repository Status

This is a single-file Python app with supporting documentation. It is ready to initialize as a Git repository and publish to GitHub once Git/GitHub authentication is available.
