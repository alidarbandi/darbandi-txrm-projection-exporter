# User Manual

## 1. Overview

Darbandi TXRM Projection Exporter extracts projection images and metadata from ZEISS Xradia `.txrm` and `.txm` files. It also provides preprocessing and geometry scanning tools for reconstruction workflows.

The main operations are:

- `Extract`: exports projections, references, dark fields, metadata, and logs.
- `Flat-field Correct`: averages reference images and divides projections by the reference average.
- `Attenuation`: converts flat-field corrected images using `p = -ln(I_corrected)`.
- `Scan Geometry`: parses `metadata/metadata.xlsx` and writes TIGRE FDK geometry files.

## 2. Installation

Install Python 3.10 or newer, then install dependencies from the repository folder:

```powershell
python -m pip install -r requirements.txt
```

Run the self-test:

```powershell
python txrm_read.py --self-test
```

You should see:

```text
Self-test passed. Full extraction validation requires a real TXRM/TXM file.
```

## 3. Starting The App

Launch the graphical interface:

```powershell
python txrm_read.py
```

The window opens with:

- input file picker
- output folder picker
- action buttons
- projection preview panel
- shift plot panel
- metadata/log tabs

## 4. Extracting A Scan

1. Click `Browse` next to `TXRM file`.
2. Select a `.txrm` or `.txm` file.
3. Choose an output folder, or leave it blank to auto-create one.
4. Click `Extract`.

The app writes projection TIFFs in native row/column order. No image flips, sign changes, or dtype conversion are applied during extraction.

## 5. Projection Preview

The Projection Preview panel shows the first and middle projection.

Use the `Source` dropdown to view:

- `Extracted projections`
- `Flat-field corrected`
- `Attenuation`

Preview images use display-only auto contrast. The saved TIFF data is not altered by preview contrast.

## 6. Flat-Field Correction

After extraction, click `Flat-field Correct`.

The app:

1. Reads all TIFFs in `reference/`.
2. Averages them into `flatfield_corrected/reference_average_float32.tif`.
3. Divides each projection by the reference average.
4. Writes `flatfield_corrected/flatfield_000001.tif`, ...

The preview automatically switches to flat-field corrected images when processing completes.

## 7. Attenuation Conversion

After flat-field correction, click `Attenuation`.

The app converts each corrected image:

```text
p = -ln(I_corrected)
```

It writes:

```text
attenuation/attenuation_000001.tif
```

The preview automatically switches to attenuation images when processing completes.

## 8. Geometry Scan

After extraction, click `Scan Geometry`.

The app reads:

```text
metadata/metadata.xlsx
```

and writes:

```text
metadata/tigre_geometry/
```

with:

- `tigre_fdk_geometry.json`
- `geometry.json`
- `tigre_fdk_geometry_summary.txt`
- `geometry_summary.txt`
- `projection_angle_table.csv`
- `cleaned_metadata_dump.json`

The geometry JSON includes:

- projection filenames and angles
- image size and dtype
- `StoRADistance`, `DtoRADistance`, `DSO`, `DOD`, and `DSD`
- `XrayMagnification`
- effective pixel size and TIGRE detector pixel size
- detector and voxel recommendations
- center shift candidates
- per-projection alignment shifts
- exposure, current, voltage, camera binning, lens/objective, field of view, temperature, cone angle, and fan angle
- warnings and source-row provenance

## 9. Command-Line Reference

Extract:

```powershell
python txrm_read.py --input "scan.txrm" --output "scan_export"
```

Scan geometry only:

```powershell
python txrm_read.py --metadata "scan_export\metadata\metadata.xlsx" --geometry-output "scan_export\metadata\tigre_geometry"
```

Self-test:

```powershell
python txrm_read.py --self-test
```

## 10. Troubleshooting

Missing dependency:

```powershell
python -m pip install -r requirements.txt
```

No projection TIFFs found:

- Make sure extraction finished successfully.
- Check that `projections/` exists inside the selected output folder.

Flat-field correction fails:

- Check that `reference/` contains TIFF images.
- Check that reference and projection image shapes match.

Geometry values look wrong:

- Open `metadata/tigre_geometry/tigre_fdk_geometry_summary.txt`.
- Check the `Warnings` section.
- Check `raw_decoded_metadata` in the JSON for source rows and decoded binary candidates.

## 11. Data Safety

The app writes new output folders and files. It does not modify the original `.txrm` or `.txm` input file.

Large microscopy data should not be committed to GitHub. Keep scan data outside the repository or in ignored output folders.
