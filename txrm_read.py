#!/usr/bin/env python
"""
TXRM Reader and Exporter

Reads ZEISS Xradia TXRM/TXM OLE2 containers, exports projection/reference/dark
images as TIFF while preserving dtype, and writes microscope/image/geometry
metadata to Excel for reconstruction workflows.
"""

from __future__ import annotations

import argparse
import ast
import json
import logging
import math
import os
import queue
import re
import struct
import sys
import tempfile
import threading
import traceback
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

try:
    import numpy as np
    import olefile
    import pandas as pd
    import tifffile
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    from matplotlib.figure import Figure
    from PIL import Image, ImageTk
except ImportError as exc:  # pragma: no cover - shown in GUI/CLI at runtime
    missing_dependency = exc
else:
    missing_dependency = None

import tkinter as tk
from tkinter import filedialog, messagebox, ttk


APP_TITLE = "Darbandi TXRM Projection Exporter"
EXCEL_CELL_LIMIT = 32767
METADATA_DECODE_LIMIT = 1024 * 1024
EXCEL_ILLEGAL_CHARACTERS_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")
METADATA_PANEL_FRACTION = 0.20
PREVIEW_CONTRAST_PERCENTILES = (1.0, 99.0)
PREVIEW_SOURCE_EXTRACTED = "Extracted projections"
PREVIEW_SOURCE_FLATFIELD = "Flat-field corrected"
PREVIEW_SOURCE_ATTENUATION = "Attenuation"
PREVIEW_SOURCES = (
    PREVIEW_SOURCE_EXTRACTED,
    PREVIEW_SOURCE_FLATFIELD,
    PREVIEW_SOURCE_ATTENUATION,
)
PREVIEW_SOURCE_FOLDERS = {
    PREVIEW_SOURCE_EXTRACTED: "projections",
    PREVIEW_SOURCE_FLATFIELD: "flatfield_corrected",
    PREVIEW_SOURCE_ATTENUATION: "attenuation",
}
APP_BACKGROUND = "#233b76"
APP_SURFACE = "#1c326a"
APP_FIELD_BACKGROUND = "#172a5d"
APP_TEXT = "#f8fbff"
APP_MUTED_TEXT = "#dbe7ff"
APP_BORDER = "#8fa8e6"
APP_BUTTON_BACKGROUND = "#2e4f99"
APP_BUTTON_ACTIVE = "#3d65ba"
APP_SELECTION = "#5578cc"
APP_TREE_HEADING = "#2a478c"
PLOT_X_COLOR = "#9cc8ff"
PLOT_Y_COLOR = "#ffb0b0"
GEOMETRY_SCAN_FOLDER = "tigre_geometry"
GEOMETRY_JSON_NAME = "tigre_fdk_geometry.json"
GEOMETRY_JSON_ALIAS_NAME = "geometry.json"
GEOMETRY_SUMMARY_NAME = "tigre_fdk_geometry_summary.txt"
GEOMETRY_SUMMARY_ALIAS_NAME = "geometry_summary.txt"
GEOMETRY_TABLE_NAME = "projection_angle_table.csv"
GEOMETRY_CLEANED_DUMP_NAME = "cleaned_metadata_dump.json"
FLOAT_PATTERN = r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?"
BINARY_ESCAPE_RE = re.compile(r"(?:\\x[0-9A-Fa-f]{2}){2,}")
COMMON_MOJIBAKE_MARKERS = ("Â", "Ã", "â", "�")


@dataclass
class MetadataEntry:
    category: str
    path: str
    parameter: str
    value: Any
    unit: str = ""
    data_type: str = ""
    count: int = 1
    notes: str = ""


@dataclass
class ImageStream:
    role: str
    path: str
    index_1based: Optional[int]
    output_name: str
    data_type_code: Optional[int]
    dtype_name: str
    width: int
    height: int
    size_bytes: int


def dependency_message() -> str:
    return (
        "Missing Python dependency: "
        f"{missing_dependency.name if missing_dependency else 'unknown'}\n\n"
        "Install required packages with:\n"
        "python -m pip install numpy olefile tifffile pandas openpyxl pillow matplotlib"
    )


def normalize_ole_path(parts_or_path: Iterable[str] | str) -> str:
    if isinstance(parts_or_path, str):
        path = parts_or_path.replace("\\", "/")
    else:
        path = "/".join(parts_or_path)
    return path.replace("Root Entry/", "").strip("/")


def path_key(path: str) -> str:
    return normalize_ole_path(path).lower()


def clean_string(data: bytes) -> str:
    data = data.split(b"\x00", 1)[0]
    for encoding in ("utf-8", "latin-1", "utf-16-le"):
        try:
            text = data.decode(encoding).strip()
        except UnicodeDecodeError:
            continue
        if text and printable_fraction(text) > 0.85:
            return text
    return ""


def printable_fraction(text: str) -> float:
    if not text:
        return 0.0
    printable = sum(1 for char in text if char.isprintable() or char in "\r\n\t")
    return printable / len(text)


def safe_sheet_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, (list, tuple)):
        if len(value) <= 12:
            text = ", ".join(format_scalar(v) for v in value)
        else:
            head = ", ".join(format_scalar(v) for v in value[:6])
            tail = ", ".join(format_scalar(v) for v in value[-3:])
            text = f"{head}, ... {tail} ({len(value)} values)"
    elif isinstance(value, dict):
        text = json.dumps(value, default=str)
    else:
        text = format_scalar(value)
    text = excel_safe_text(text)
    if len(text) > EXCEL_CELL_LIMIT:
        text = text[: EXCEL_CELL_LIMIT - 24] + " ... [truncated]"
    return text


def excel_safe_text(value: Any) -> str:
    text = "" if value is None else str(value)
    return EXCEL_ILLEGAL_CHARACTERS_RE.sub(" ", text)


def sanitize_dataframe_for_excel(frame: "pd.DataFrame") -> "pd.DataFrame":
    if frame.empty:
        return frame
    clean = frame.copy()
    object_columns = clean.select_dtypes(include=["object"]).columns
    for column in object_columns:
        clean[column] = clean[column].map(lambda value: excel_safe_text(value) if value is not None else value)
    return clean


def format_scalar(value: Any) -> str:
    if isinstance(value, float):
        if math.isnan(value):
            return ""
        return f"{value:.9g}"
    return str(value)


def sanitize_filename_token(text: Any) -> str:
    token = str(text)
    token = token.replace("-", "m").replace("+", "p")
    token = re.sub(r"[^A-Za-z0-9_.-]+", "_", token)
    return token.strip("_") or "value"


class TXRMReader:
    """OLE2-based reader for ZEISS Xradia TXRM/TXM files."""

    projection_re = re.compile(r"^imagedata(\d+)/image(\d+)$", re.IGNORECASE)

    scalar_specs = {
        "imageinfo/noofimages": ("Image", "Number of images", "<I", ""),
        "imageinfo/imagewidth": ("Image", "Image width", "<I", "px"),
        "imageinfo/imageheight": ("Image", "Image height", "<I", "px"),
        "imageinfo/datatype": ("Image", "Data type code", "<I", ""),
        "imageinfo/filetype": ("Image", "File type", "string", ""),
        "imageinfo/pixelsize": ("Image", "Pixel size", "<f", "um"),
        "imageinfo/pixel size": ("Image", "Pixel size", "<f", "um"),
        "imageinfo/acquisitionmode": ("Image", "Acquisition mode", "<I", ""),
        "imageinfo/cameranumberofframesperimage": (
            "Image",
            "Camera frames per image",
            "<I",
            "",
        ),
        "imageinfo/noofimagesaveraged": ("Image", "Images averaged", "<I", ""),
        "imageinfo/camerabinning": ("Image", "Camera binning", "<I", ""),
        "imageinfo/referencefile": ("Reference", "Reference filename", "string", ""),
        "imageinfo/xraymagnification": ("Geometry", "XrayMagnification", "<f", ""),
        "imageinfo/x-raymagnification": ("Geometry", "XrayMagnification", "<f", ""),
        "imageinfo/coneangle": ("Geometry", "Cone angle", "<f", "deg"),
        "imageinfo/fanangle": ("Geometry", "Fan angle", "<f", "deg"),
        "imageinfo/exposuretime": ("Acquisition", "Exposure time", "<f", "s"),
        "imageinfo/temperature": ("Acquisition", "Temperature", "<f", ""),
        "imageinfo/cameratemperature": ("Acquisition", "Camera temperature", "<f", ""),
        "referencedata/datatype": ("Reference", "Reference data type code", "<I", ""),
        "referencedata/data type": ("Reference", "Reference data type code", "<I", ""),
        "darkfielddata/datatype": ("Dark field", "Dark-field data type code", "<I", ""),
        "darkdata/datatype": ("Dark field", "Dark-field data type code", "<I", ""),
        "sampleinfo/facility": ("Microscope", "Facility", "string", ""),
        "exeversion": ("Dataset", "Executable version", "string", ""),
        "detassemblyinfo/lensinfo/lensname": ("Microscope", "Objective / lens", "string", ""),
        "imageinfo/sourcefiltername": ("Microscope", "Source filter name", "string", ""),
        "imageinfo/voltage": ("Microscope", "Voltage", "<f", "kV"),
    }

    array_specs = {
        "imageinfo/angles": ("Geometry", "Projection angle", "float32", "deg"),
        "imageinfo/xposition": ("Geometry", "X position", "float32", ""),
        "imageinfo/yposition": ("Geometry", "Y position", "float32", ""),
        "imageinfo/zposition": ("Geometry", "Z position", "float32", ""),
        "alignment/x-shifts": ("Alignment", "X shift", "float32", "px"),
        "alignment/y-shifts": ("Alignment", "Y shift", "float32", "px"),
        "imageinfo/exptimes": ("Acquisition", "Exposure time", "float32", "s"),
        "imageinfo/current": ("Microscope", "X-ray current", "float32", "uA"),
        "imageinfo/xrayvoltage": ("Microscope", "X-ray voltage", "float32", "kV"),
        "imageinfo/datastamps": ("Acquisition", "Date stamp", "string-array", ""),
    }

    def __init__(self, filename: Path):
        self.filename = Path(filename)
        self.ole: Optional[olefile.OleFileIO] = None
        self.stream_paths: list[str] = []
        self.entries: list[MetadataEntry] = []
        self.entry_by_key: dict[str, MetadataEntry] = {}
        self.metadata: dict[str, Any] = {}
        self.image_streams: list[ImageStream] = []

    def __enter__(self) -> "TXRMReader":
        self.open()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def open(self) -> None:
        self.ole = olefile.OleFileIO(str(self.filename))
        self.stream_paths = sorted(
            normalize_ole_path(parts) for parts in self.ole.listdir(streams=True, storages=False)
        )
        if not self.stream_paths:
            raise ValueError("No streams were found. The TXRM file may be corrupt.")
        self._read_metadata()
        self.image_streams = self._discover_image_streams()

    def close(self) -> None:
        if self.ole is not None:
            self.ole.close()
            self.ole = None

    def exists(self, path: str) -> bool:
        assert self.ole is not None
        return self.ole.exists(normalize_ole_path(path))

    def stream_size(self, path: str) -> int:
        assert self.ole is not None
        return int(self.ole.get_size(normalize_ole_path(path)))

    def read_stream(self, path: str) -> bytes:
        assert self.ole is not None
        with self.ole.openstream(normalize_ole_path(path)) as stream:
            return stream.read()

    def read_image(self, image_stream: ImageStream) -> "np.ndarray":
        raw = self.read_stream(image_stream.path)
        dtype = self.dtype_for_code(image_stream.data_type_code)
        expected_items = image_stream.width * image_stream.height
        arr = np.frombuffer(raw, dtype=dtype, count=expected_items)
        if arr.size != expected_items:
            raise ValueError(
                f"{image_stream.path} contains {arr.size} pixels, expected {expected_items}."
            )
        # Preserve the native TXRM row/column order. Do not flip axes here; geometry
        # arrays are exported separately for reconstruction code to consume directly.
        return arr.reshape((image_stream.height, image_stream.width))

    def _read_metadata(self) -> None:
        for path in self.stream_paths:
            key = path_key(path)
            if self.projection_re.match(key):
                continue
            size = self.stream_size(path)
            if size > METADATA_DECODE_LIMIT:
                self._add_entry("Streams", path, "Large stream", f"{size} bytes", "bytes", "bytes")
                continue
            data = self.read_stream(path)
            entry = self._decode_known_or_generic(path, data)
            self._add(entry)
        self._populate_core_metadata()
        self._add_derived_metadata()

    def _decode_known_or_generic(self, path: str, data: bytes) -> MetadataEntry:
        key = path_key(path)
        if key in self.scalar_specs:
            category, parameter, fmt, unit = self.scalar_specs[key]
            value = self._decode_scalar(data, fmt)
            return MetadataEntry(category, path, parameter, value, unit, fmt, 1)
        if key in self.array_specs:
            category, parameter, dtype_name, unit = self.array_specs[key]
            value = self._decode_array(data, dtype_name)
            return MetadataEntry(category, path, parameter, value, unit, dtype_name, len(value))
        return self._decode_generic(path, data)

    def _decode_scalar(self, data: bytes, fmt: str) -> Any:
        if fmt == "string":
            return clean_string(data)
        size = struct.calcsize(fmt)
        if len(data) < size:
            return None
        return struct.unpack(fmt, data[:size])[0]

    def _decode_array(self, data: bytes, dtype_name: str) -> list[Any]:
        if dtype_name == "string-array":
            text = clean_string(data)
            return [part.strip() for part in re.split(r"[\r\n\t\x00]+", text) if part.strip()]
        if dtype_name == "float32":
            if len(data) % 4 != 0:
                return []
            return np.frombuffer(data, dtype="<f4").astype(float).tolist()
        if dtype_name == "uint32":
            if len(data) % 4 != 0:
                return []
            return np.frombuffer(data, dtype="<u4").astype(int).tolist()
        return []

    def _decode_generic(self, path: str, data: bytes) -> MetadataEntry:
        text = clean_string(data)
        if len(data) == 4:
            value = {
                "text": text,
                "uint32": struct.unpack("<I", data)[0],
                "int32": struct.unpack("<i", data)[0],
                "float32": struct.unpack("<f", data)[0],
                "hex": data.hex(),
            }
            return MetadataEntry("Metadata", path, Path(path).name, value, "", "uint32/int32/float32/text", 1)
        if len(data) == 8:
            value = {
                "text": text,
                "uint64": struct.unpack("<Q", data)[0],
                "int64": struct.unpack("<q", data)[0],
                "float64": struct.unpack("<d", data)[0],
                "float32_pair": [
                    struct.unpack("<f", data[:4])[0],
                    struct.unpack("<f", data[4:])[0],
                ],
                "hex": data.hex(),
            }
            return MetadataEntry("Metadata", path, Path(path).name, value, "", "uint64/int64/float64/float32/text", 1)
        if text:
            return MetadataEntry("Metadata", path, Path(path).name, text, "", "string", 1)
        if len(data) == 1:
            return MetadataEntry("Metadata", path, Path(path).name, data[0], "", "uint8", 1)
        if len(data) % 4 == 0 and len(data) <= 16384:
            floats = np.frombuffer(data, dtype="<f4").astype(float)
            if floats.size and np.isfinite(floats).all() and np.nanmax(np.abs(floats)) < 1e12:
                return MetadataEntry(
                    "Metadata",
                    path,
                    Path(path).name,
                    floats.tolist(),
                    "",
                    "float32[]",
                    int(floats.size),
                )
        return MetadataEntry("Metadata", path, Path(path).name, f"{len(data)} raw bytes", "bytes", "binary", 1)

    def _add(self, entry: MetadataEntry) -> None:
        self.entries.append(entry)
        self.entry_by_key[path_key(entry.path)] = entry

    def _add_entry(
        self,
        category: str,
        path: str,
        parameter: str,
        value: Any,
        unit: str = "",
        data_type: str = "",
        notes: str = "",
    ) -> None:
        self._add(MetadataEntry(category, path, parameter, value, unit, data_type, 1, notes))

    def _populate_core_metadata(self) -> None:
        def get_value(*keys: str) -> Any:
            for key in keys:
                entry = self.entry_by_key.get(key.lower())
                if entry is not None:
                    return entry.value
            return None

        self.metadata = {
            "number_of_images": get_value("imageinfo/noofimages"),
            "image_width": get_value("imageinfo/imagewidth"),
            "image_height": get_value("imageinfo/imageheight"),
            "data_type": get_value("imageinfo/datatype"),
            "pixel_size_um": get_value("imageinfo/pixelsize", "imageinfo/pixel size"),
            "reference_filename": get_value("imageinfo/referencefile"),
            "angles_deg": get_value("imageinfo/angles") or [],
            "x_positions": get_value("imageinfo/xposition") or [],
            "y_positions": get_value("imageinfo/yposition") or [],
            "z_positions": get_value("imageinfo/zposition") or [],
            "x_shifts_px": get_value("alignment/x-shifts") or [],
            "y_shifts_px": get_value("alignment/y-shifts") or [],
            "exposure_s": get_value("imageinfo/exptimes") or [],
            "current_uA": get_value("imageinfo/current") or [],
            "voltage_kV": get_value("imageinfo/xrayvoltage") or [],
        }

        for name in ("number_of_images", "image_width", "image_height", "data_type"):
            value = self.metadata[name]
            if value is not None:
                self.metadata[name] = int(value)

    def _add_derived_metadata(self) -> None:
        width = self.metadata.get("image_width")
        height = self.metadata.get("image_height")
        pixel_size = self.metadata.get("pixel_size_um")
        if width and height and pixel_size:
            self._add_entry("Image", "Derived/FOV_X", "Field of view X", width * pixel_size, "um", "derived")
            self._add_entry("Image", "Derived/FOV_Y", "Field of view Y", height * pixel_size, "um", "derived")
        dtype_code = self.metadata.get("data_type")
        if dtype_code is not None:
            self._add_entry(
                "Image",
                "Derived/NumPy_dtype",
                "NumPy dtype",
                str(self.dtype_for_code(dtype_code)),
                "",
                "derived",
            )
        self._add_entry(
            "Geometry",
            "Derived/CoordinateSystemNote",
            "Coordinate system note",
            (
                "Image arrays are exported in native TXRM row/column order. "
                "ImageInfo/XPosition, YPosition, ZPosition, Angles, and alignment shifts "
                "are copied from the file without sign changes or axis swaps. Angles are "
                "also exported in radians for ASTRA-style reconstruction scripts."
            ),
            "",
            "note",
        )

    def _discover_image_streams(self) -> list[ImageStream]:
        width = self.metadata.get("image_width")
        height = self.metadata.get("image_height")
        dtype_code = self.metadata.get("data_type")
        if not width or not height or dtype_code is None:
            raise ValueError("ImageInfo/ImageWidth, ImageHeight, or DataType is missing.")
        dtype = self.dtype_for_code(dtype_code)

        streams: list[ImageStream] = []
        for path in self.stream_paths:
            key = path_key(path)
            match = self.projection_re.match(key)
            if not match:
                continue
            index_1based = int(match.group(2))
            streams.append(
                ImageStream(
                    role="projection",
                    path=path,
                    index_1based=index_1based,
                    output_name=f"proj_{index_1based:06d}.tif",
                    data_type_code=dtype_code,
                    dtype_name=str(dtype),
                    width=int(width),
                    height=int(height),
                    size_bytes=self.stream_size(path),
                )
            )
        streams.sort(key=lambda item: item.index_1based or 0)

        found_projection_paths = {stream.path for stream in streams}
        for path in self.stream_paths:
            if path in found_projection_paths:
                continue
            key = path_key(path)
            size = self.stream_size(path)
            role = self._classify_aux_image(key)
            if role is None:
                continue
            aux_dtype_code = self._dtype_code_for_aux_image(key, dtype_code)
            aux_dtype = self.dtype_for_code(aux_dtype_code)
            aux_expected_size = int(width) * int(height) * aux_dtype.itemsize
            if size != aux_expected_size:
                continue
            count_for_role = sum(1 for stream in streams if stream.role == role) + 1
            streams.append(
                ImageStream(
                    role=role,
                    path=path,
                    index_1based=count_for_role,
                    output_name=f"{role}_{count_for_role:03d}.tif",
                    data_type_code=aux_dtype_code,
                    dtype_name=str(aux_dtype),
                    width=int(width),
                    height=int(height),
                    size_bytes=size,
                )
            )
        return streams

    def _classify_aux_image(self, key: str) -> Optional[str]:
        if "dark" in key:
            return "dark_field"
        if "reference" in key or "flat" in key or key in {"referencedata/image", "reference/image"}:
            return "reference"
        if key.endswith("/image") or "image" in key:
            return "auxiliary"
        return None

    def _dtype_code_for_aux_image(self, key: str, fallback: int) -> int:
        candidate_keys: list[str] = []
        if "reference" in key:
            candidate_keys.extend(["referencedata/datatype", "referencedata/data type"])
        if "dark" in key:
            candidate_keys.extend(["darkfielddata/datatype", "darkdata/datatype"])
        for candidate in candidate_keys:
            entry = self.entry_by_key.get(candidate)
            if entry is not None and entry.value is not None:
                try:
                    return int(entry.value)
                except (TypeError, ValueError):
                    continue
        return int(fallback)

    @staticmethod
    def dtype_for_code(code: Optional[int]) -> "np.dtype":
        mapping = {
            1: np.dtype("<u1"),
            2: np.dtype("<u1"),
            3: np.dtype("<i2"),
            4: np.dtype("<u2"),
            5: np.dtype("<u2"),
            6: np.dtype("<i4"),
            7: np.dtype("<u4"),
            9: np.dtype("<f4"),
            10: np.dtype("<f4"),
            11: np.dtype("<f8"),
        }
        if code not in mapping:
            raise ValueError(f"Unsupported or unknown TXRM data type code: {code}")
        return mapping[code]

    def projection_geometry_rows(self) -> list[dict[str, Any]]:
        projections = [stream for stream in self.image_streams if stream.role == "projection"]
        rows: list[dict[str, Any]] = []
        for seq, stream in enumerate(projections, start=1):
            index = (stream.index_1based or seq) - 1
            angle_deg = value_at(self.metadata.get("angles_deg"), index)
            rows.append(
                {
                    "sequence": seq,
                    "zeiss_image_index_1based": stream.index_1based,
                    "stream_path": stream.path,
                    "tiff_file": stream.output_name,
                    "angle_deg": angle_deg,
                    "angle_rad": math.radians(angle_deg) if angle_deg is not None else None,
                    "x_position": value_at(self.metadata.get("x_positions"), index),
                    "y_position": value_at(self.metadata.get("y_positions"), index),
                    "z_position": value_at(self.metadata.get("z_positions"), index),
                    "x_shift_px": value_at(self.metadata.get("x_shifts_px"), index),
                    "y_shift_px": value_at(self.metadata.get("y_shifts_px"), index),
                    "exposure_s": value_at(self.metadata.get("exposure_s"), index),
                    "current_uA": value_at(self.metadata.get("current_uA"), index),
                    "voltage_kV": value_at(self.metadata.get("voltage_kV"), index),
                    "width_px": stream.width,
                    "height_px": stream.height,
                    "dtype": stream.dtype_name,
                }
            )
        return rows


def value_at(values: Any, index: int) -> Any:
    if values is None:
        return None
    if isinstance(values, np.ndarray):
        values = values.tolist()
    if not isinstance(values, (list, tuple)) or not values:
        return None
    if len(values) == 1:
        return values[0]
    if 0 <= index < len(values):
        return values[index]
    return None


def to_finite_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def extract_shift_points(geometry_rows: list[dict[str, Any]], shift_column: str) -> tuple[list[float], list[float]]:
    angles: list[float] = []
    shifts: list[float] = []
    for row in geometry_rows:
        angle = to_finite_float(row.get("angle_deg"))
        shift = to_finite_float(row.get(shift_column))
        if angle is None or shift is None:
            continue
        angles.append(angle)
        shifts.append(shift)
    return angles, shifts


def tiff_sort_key(path: Path) -> tuple[Any, ...]:
    numbers = re.findall(r"\d+", path.stem)
    if numbers:
        return (0, *[int(number) for number in numbers], path.name.lower())
    return (1, path.name.lower())


def list_tiff_files(folder: Path) -> list[Path]:
    if not folder.exists():
        return []
    files = [path for pattern in ("*.tif", "*.tiff") for path in folder.glob(pattern)]
    return sorted(files, key=tiff_sort_key)


def projection_output_name(source: Path, prefix: str) -> str:
    match = re.search(r"(\d+)", source.stem)
    if match:
        return f"{prefix}_{int(match.group(1)):06d}.tif"
    return f"{prefix}_{source.stem}.tif"


def preview_files_for_source(extract_dir: Path, source: str) -> list[Path]:
    folder_name = PREVIEW_SOURCE_FOLDERS.get(source, PREVIEW_SOURCE_FOLDERS[PREVIEW_SOURCE_EXTRACTED])
    files = list_tiff_files(Path(extract_dir) / folder_name)
    if source == PREVIEW_SOURCE_FLATFIELD:
        return [path for path in files if not path.name.lower().startswith("reference_average")]
    return files


def clean_metadata_key_or_value(x: Any) -> str:
    """Clean hidden characters and metadata artifacts while preserving useful text."""
    if x is None:
        return ""
    try:
        if isinstance(x, float) and math.isnan(x):
            return ""
    except TypeError:
        pass
    s = repair_metadata_mojibake(str(x))
    s = s.replace("\x00", "")
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u00A0", " ")
    s = "".join(char for char in s if char.isprintable() or char in "\r\n\t ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def repair_metadata_mojibake(text: str) -> str:
    """Repair common Excel/Latin-1/UTF-8 artifacts without dropping meaning."""
    if not text:
        return ""
    repaired = text
    if any(marker in repaired for marker in COMMON_MOJIBAKE_MARKERS):
        for source_encoding in ("latin-1", "cp1252"):
            try:
                candidate = repaired.encode(source_encoding).decode("utf-8")
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue
            if candidate.count("�") <= repaired.count("�"):
                repaired = candidate
                break
    repaired = unicodedata.normalize("NFKC", repaired)
    replacements = {
        "\u00A0": " ",
        "\u2212": "-",
        "\u2010": "-",
        "\u2011": "-",
        "\u2012": "-",
        "\u2013": "-",
        "\u2014": "-",
        "\u00B5": "u",
        "\u03BC": "u",
        "\u00D7": "x",
        "\u00B0": " deg ",
    }
    for old, new in replacements.items():
        repaired = repaired.replace(old, new)
    return repaired


def normalize_key_for_matching(s: Any) -> str:
    s = clean_metadata_key_or_value(s).lower()
    s = s.replace("_", "")
    s = s.replace("-", "")
    s = s.replace("/", "")
    s = s.replace("\\", "")
    s = s.replace(" ", "")
    s = s.replace(":", "")
    s = s.replace(".", "")
    s = s.replace("(", "")
    s = s.replace(")", "")
    return s


def json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    return value


def raw_metadata_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if isinstance(value, float) and math.isnan(value):
            return ""
    except TypeError:
        pass
    return str(value)


def bytes_from_metadata_text(text: str) -> list[tuple[str, bytes]]:
    candidates: list[tuple[str, bytes]] = []
    if not text:
        return candidates
    stripped = text.strip()
    if BINARY_ESCAPE_RE.search(stripped):
        try:
            candidates.append(("escaped_hex", stripped.encode("latin-1").decode("unicode_escape").encode("latin-1")))
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
    if stripped.startswith(("b'", 'b"')):
        try:
            literal = ast.literal_eval(stripped)
        except (SyntaxError, ValueError):
            literal = None
        if isinstance(literal, bytes):
            candidates.append(("python_bytes_literal", literal))
    if len(stripped) in (4, 8) and any(ord(char) > 126 or ord(char) < 32 for char in stripped):
        for encoding in ("latin-1", "cp1252"):
            try:
                candidates.append((f"{encoding}_text_bytes", stripped.encode(encoding)))
            except UnicodeEncodeError:
                continue
    plain_numeric_list = re.fullmatch(r"[0-9eE+\-.,\s\[\]()]+", stripped) is not None
    if len(stripped) >= 4 and len(stripped) % 4 == 0 and not plain_numeric_list:
        for encoding in ("latin-1", "cp1252", "utf-16-le"):
            try:
                candidates.append((f"{encoding}_payload_bytes", stripped.encode(encoding)))
            except UnicodeEncodeError:
                continue
    unique: list[tuple[str, bytes]] = []
    seen: set[bytes] = set()
    for source, data in candidates:
        if data not in seen:
            seen.add(data)
            unique.append((source, data))
    return unique


def binary_float32_series_candidates(value: Any) -> list[dict[str, Any]]:
    series_candidates: list[dict[str, Any]] = []
    for source, data in bytes_from_metadata_text(raw_metadata_text(value)):
        if len(data) < 4 or len(data) % 4 != 0:
            continue
        try:
            values = np.frombuffer(data, dtype="<f4").astype(float)
        except ValueError:
            continue
        finite = values[np.isfinite(values)]
        if not finite.size:
            continue
        if np.nanmax(np.abs(finite)) > 1e12:
            continue
        series_candidates.append(
            {
                "source": source,
                "type": "float32[]",
                "count": int(values.size),
                "values": values.tolist(),
                "first": float(values[0]),
                "min": float(np.min(finite)),
                "max": float(np.max(finite)),
                "mean": float(np.mean(finite)),
                "hex_prefix": data[: min(len(data), 32)].hex(),
            }
        )
    return series_candidates


def preferred_binary_float32_series(value: Any) -> list[float]:
    candidates = binary_float32_series_candidates(value)
    if not candidates:
        return []
    repeated_candidates = [
        candidate
        for candidate in candidates
        if candidate["count"] > 1 and abs(candidate["max"] - candidate["min"]) < 1e-5 * max(1.0, abs(candidate["mean"]))
    ]
    candidate = repeated_candidates[0] if repeated_candidates else candidates[0]
    return [float(item) for item in candidate["values"]]


def looks_like_binary_payload(value: Any) -> bool:
    text = raw_metadata_text(value).strip()
    if not text:
        return False
    if BINARY_ESCAPE_RE.search(text) or text.startswith(("b'", 'b"')):
        return True
    if len(text) >= 4 and len(text) % 4 == 0:
        if re.fullmatch(r"[0-9eE+\-.,\s\[\]()]+", text):
            return False
        if any(ord(char) > 126 or ord(char) < 32 for char in text):
            return True
        chunks = [text[index : index + 4] for index in range(0, len(text), 4)]
        if len(chunks) > 1 and len(set(chunks)) <= max(2, len(chunks) // 4):
            return True
        if len(text) == 4 and any(char in text for char in (":", "@", "\x00")):
            return True
    return False


def binary_numeric_candidates(value: Any) -> list[dict[str, Any]]:
    text = raw_metadata_text(value)
    candidates: list[dict[str, Any]] = []
    for source, data in bytes_from_metadata_text(text):
        for offset in (0,):
            chunk = data[offset:]
            if len(chunk) >= 4:
                first4 = chunk[:4]
                for endian, prefix in (("little", "<"), ("big", ">")):
                    for type_name, fmt in (
                        ("float32", "f"),
                        ("int32", "i"),
                        ("uint32", "I"),
                    ):
                        try:
                            number = struct.unpack(prefix + fmt, first4)[0]
                        except struct.error:
                            continue
                        if isinstance(number, float) and not math.isfinite(number):
                            continue
                        candidates.append(
                            {
                                "source": source,
                                "encoding": endian,
                                "type": type_name,
                                "value": float(number) if isinstance(number, float) else int(number),
                                "hex": first4.hex(),
                            }
                        )
            if len(chunk) >= 8:
                first8 = chunk[:8]
                for endian, prefix in (("little", "<"), ("big", ">")):
                    for type_name, fmt in (
                        ("float64", "d"),
                        ("int64", "q"),
                        ("uint64", "Q"),
                    ):
                        try:
                            number = struct.unpack(prefix + fmt, first8)[0]
                        except struct.error:
                            continue
                        if isinstance(number, float) and not math.isfinite(number):
                            continue
                        candidates.append(
                            {
                                "source": source,
                                "encoding": endian,
                                "type": type_name,
                                "value": float(number) if isinstance(number, float) else int(number),
                                "hex": first8.hex(),
                            }
                        )
    return candidates


def preferred_binary_numeric(value: Any, prefer_integer: bool = False) -> Optional[float]:
    candidates = binary_numeric_candidates(value)
    if not candidates:
        return None
    if prefer_integer:
        type_order = ("uint32", "int32", "uint64", "int64", "float32", "float64")
    else:
        type_order = ("float32", "float64", "int32", "uint32", "int64", "uint64")
    for type_name in type_order:
        for candidate in candidates:
            number = candidate["value"]
            if candidate["type"] != type_name:
                continue
            if isinstance(number, (int, float)) and math.isfinite(float(number)) and abs(float(number)) < 1e12:
                return float(number)
    return None


def metadata_cell_interpretation(value: Any) -> dict[str, Any]:
    original = raw_metadata_text(value)
    repaired = repair_metadata_mojibake(original)
    cleaned = clean_metadata_key_or_value(value)
    binary_candidates = binary_numeric_candidates(value)
    binary_series_candidates = binary_float32_series_candidates(value)
    numeric_from_text = None
    numbers = re.findall(FLOAT_PATTERN, cleaned)
    if numbers:
        numeric_from_text = float(numbers[0])
    return {
        "original_repr": repr(original),
        "cleaned": cleaned,
        "repaired_text": repaired if repaired != original else "",
        "numeric_from_text": numeric_from_text,
        "binary_numeric_candidates": binary_candidates[:12],
        "binary_float32_series_candidates": [
            {key: item[key] for key in ("source", "type", "count", "first", "min", "max", "mean", "hex_prefix")}
            for item in binary_series_candidates[:4]
        ],
        "selected_binary_numeric": preferred_binary_numeric(value),
        "selected_binary_float32_series_first": (
            preferred_binary_float32_series(value)[0] if preferred_binary_float32_series(value) else None
        ),
    }


def parse_numeric_value(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, dict):
        if "uint32" in value and "float32" in value:
            float32_value = parse_numeric_value(value.get("float32"))
            uint32_value = parse_numeric_value(value.get("uint32"))
            if float32_value is not None and uint32_value is not None and abs(float32_value) < 1e-30:
                return uint32_value
        for key in ("float32", "float64", "float", "value"):
            if key in value:
                parsed = parse_numeric_value(value[key])
                if parsed is not None:
                    return parsed
        for item in value.values():
            parsed = parse_numeric_value(item)
            if parsed is not None:
                return parsed
        return None

    text = clean_metadata_key_or_value(value)
    if not text:
        return None
    if looks_like_binary_payload(value):
        series = preferred_binary_float32_series(value)
        if series:
            return series[0]
        decoded_number = preferred_binary_numeric(value)
        if decoded_number is not None:
            return decoded_number
    try:
        loaded = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        loaded = None
    if isinstance(loaded, dict):
        return parse_numeric_value(loaded)

    float_match = re.search(rf'"?float(?:32|64)?"?\s*:\s*({FLOAT_PATTERN})', text, re.IGNORECASE)
    if float_match:
        return float(float_match.group(1))
    numbers = re.findall(FLOAT_PATTERN, text)
    if not numbers:
        return preferred_binary_numeric(value)
    number = float(numbers[0])
    return number if math.isfinite(number) else None


def parse_numeric_series(value: Any) -> list[float]:
    if value is None:
        return []
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, (list, tuple)):
        parsed = [parse_numeric_value(item) for item in value]
        return [number for number in parsed if number is not None]
    text = clean_metadata_key_or_value(value)
    if not text:
        return []
    try:
        loaded = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        loaded = None
    if isinstance(loaded, (list, tuple)):
        return parse_numeric_series(loaded)
    if isinstance(loaded, dict):
        number = parse_numeric_value(loaded)
        return [] if number is None else [number]
    binary_series = preferred_binary_float32_series(value)
    if binary_series:
        return binary_series
    numbers = [float(number) for number in re.findall(FLOAT_PATTERN, text)]
    text_without_counts = re.sub(r"\(\s*\d+\s+values?\s*\)", "", text, flags=re.IGNORECASE)
    numbers = [float(number) for number in re.findall(FLOAT_PATTERN, text_without_counts)]
    if not numbers:
        decoded_number = preferred_binary_numeric(value)
        return [] if decoded_number is None else [decoded_number]
    return [number for number in numbers if math.isfinite(number)]


def parse_dtype_value(value: Any) -> str:
    text = clean_metadata_key_or_value(value).lower()
    if not text:
        return ""
    dtype_aliases = {
        "uint8": ("uint8", "<u1", "|u1"),
        "int16": ("int16", "<i2"),
        "uint16": ("uint16", "<u2"),
        "int32": ("int32", "<i4"),
        "uint32": ("uint32", "<u4"),
        "float32": ("float32", "<f4"),
        "float64": ("float64", "<f8"),
    }
    for dtype_name, aliases in dtype_aliases.items():
        if any(alias in text for alias in aliases):
            return dtype_name
    code = parse_numeric_value(value)
    integer_code = preferred_binary_numeric(value, prefer_integer=True)
    if integer_code is not None and abs(integer_code - round(integer_code)) < 1e-9:
        code = integer_code
    if code is None:
        return text
    try:
        return str(TXRMReader.dtype_for_code(int(code)))
    except (ValueError, TypeError):
        return text


def find_column(frame: "pd.DataFrame", aliases: Iterable[str]) -> Optional[Any]:
    normalized_aliases = [normalize_key_for_matching(alias) for alias in aliases]
    columns = list(frame.columns)
    for column in columns:
        normalized = normalize_key_for_matching(column)
        if normalized in normalized_aliases:
            return column
    for column in columns:
        normalized = normalize_key_for_matching(column)
        if any(alias and alias in normalized for alias in normalized_aliases):
            return column
    return None


def is_blank_excel_value(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def cleaned_cell(value: Any, warnings_state: dict[str, int]) -> str:
    original = "" if is_blank_excel_value(value) else str(value)
    cleaned = clean_metadata_key_or_value(value)
    if original and cleaned != original.strip():
        warnings_state["cleaned_changes"] = warnings_state.get("cleaned_changes", 0) + 1
    return cleaned


def read_metadata_workbook(
    metadata_path: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any], list[tuple[str, "pd.DataFrame"]], list[str]]:
    warnings: list[str] = []
    warnings_state: dict[str, int] = {}
    xls = pd.ExcelFile(metadata_path)
    kv_entries: list[dict[str, Any]] = []
    cleaned_dump: dict[str, Any] = {}
    header_frames: list[tuple[str, "pd.DataFrame"]] = []

    for sheet_name in xls.sheet_names:
        try:
            raw_frame = pd.read_excel(metadata_path, sheet_name=sheet_name, header=None, dtype=object)
        except Exception as exc:
            warnings.append(f"Could not read sheet {sheet_name!r} without headers: {exc}")
            raw_frame = pd.DataFrame()
        try:
            header_frame = pd.read_excel(metadata_path, sheet_name=sheet_name, dtype=object)
        except Exception as exc:
            warnings.append(f"Could not read sheet {sheet_name!r} with headers: {exc}")
            header_frame = pd.DataFrame()

        header_frames.append((sheet_name, header_frame))
        cleaned_header_rows: list[dict[str, str]] = []
        decoded_cells: list[dict[str, Any]] = []
        for row_index, row in header_frame.iterrows():
            cleaned_row: dict[str, str] = {}
            for column, value in row.items():
                cleaned_key = cleaned_cell(column, warnings_state)
                cleaned_value = cleaned_cell(value, warnings_state)
                interpretation = metadata_cell_interpretation(value)
                if (
                    interpretation["repaired_text"]
                    or interpretation["binary_numeric_candidates"]
                    or interpretation["cleaned"] != raw_metadata_text(value).strip()
                ):
                    decoded_cells.append(
                        {
                            "sheet_mode": "header",
                            "row": int(row_index) + 2,
                            "column": clean_metadata_key_or_value(column),
                            "interpretation": interpretation,
                        }
                    )
                if cleaned_key or cleaned_value:
                    cleaned_row[cleaned_key or f"column_{len(cleaned_row) + 1}"] = cleaned_value
            if cleaned_row:
                cleaned_header_rows.append(cleaned_row)
        cleaned_raw_rows: list[list[str]] = []
        for row_index, row in raw_frame.iterrows():
            cleaned_row = [cleaned_cell(value, warnings_state) for value in row.tolist()]
            for column_index, value in enumerate(row.tolist(), start=1):
                interpretation = metadata_cell_interpretation(value)
                if (
                    interpretation["repaired_text"]
                    or interpretation["binary_numeric_candidates"]
                    or interpretation["cleaned"] != raw_metadata_text(value).strip()
                ):
                    decoded_cells.append(
                        {
                            "sheet_mode": "raw",
                            "row": int(row_index) + 1,
                            "column": int(column_index),
                            "interpretation": interpretation,
                        }
                    )
            if any(cleaned_row):
                cleaned_raw_rows.append(cleaned_row)
        cleaned_dump[sheet_name] = {
            "header_rows": cleaned_header_rows,
            "raw_rows": cleaned_raw_rows,
            "decoded_unusual_cells": decoded_cells,
        }

        add_key_value_entries_from_header(sheet_name, header_frame, kv_entries)
        add_key_value_entries_from_raw(sheet_name, raw_frame, kv_entries)

    cleaned_count = warnings_state.get("cleaned_changes", 0)
    if cleaned_count:
        warnings.append(f"{cleaned_count} metadata keys/values contained hidden or unusual characters and were cleaned.")
    decoded_count = sum(
        len(sheet_dump.get("decoded_unusual_cells", []))
        for sheet_dump in cleaned_dump.values()
        if isinstance(sheet_dump, dict)
    )
    if decoded_count:
        warnings.append(f"{decoded_count} metadata cells were preserved with decoding notes in cleaned_metadata_dump.json.")
    return kv_entries, cleaned_dump, header_frames, warnings


def add_key_value_entries_from_header(
    sheet_name: str,
    frame: "pd.DataFrame",
    kv_entries: list[dict[str, Any]],
) -> None:
    if frame.empty:
        return
    parameter_col = find_column(frame, ("parameter", "name", "key", "field"))
    value_col = find_column(frame, ("value", "data", "setting"))
    path_col = find_column(frame, ("path", "stream_path", "stream", "metadata_path"))
    unit_col = find_column(frame, ("unit", "units"))
    category_col = find_column(frame, ("category", "group", "section"))
    data_type_col = find_column(frame, ("data_type", "dtype", "type"))

    if value_col is None:
        return
    for row_index, row in frame.iterrows():
        value = row.get(value_col)
        unit = row.get(unit_col) if unit_col is not None else ""
        category = row.get(category_col) if category_col is not None else ""
        data_type = row.get(data_type_col) if data_type_col is not None else ""
        key_candidates = []
        source_path = ""
        if parameter_col is not None:
            key_candidates.append(row.get(parameter_col))
        if path_col is not None:
            path_text = clean_metadata_key_or_value(row.get(path_col))
            source_path = path_text
            key_candidates.append(path_text)
            if path_text:
                key_candidates.append(Path(path_text).name)
        if category_col is not None and parameter_col is not None:
            key_candidates.append(f"{row.get(category_col)} {row.get(parameter_col)}")
        for key in key_candidates:
            clean_key = clean_metadata_key_or_value(key)
            if clean_key:
                kv_entries.append(
                    {
                        "key": clean_key,
                        "normalized_key": normalize_key_for_matching(clean_key),
                        "value": value,
                        "unit": clean_metadata_key_or_value(unit),
                        "category": clean_metadata_key_or_value(category),
                        "data_type": clean_metadata_key_or_value(data_type),
                        "path": source_path,
                        "value_decoding": metadata_cell_interpretation(value),
                        "sheet": sheet_name,
                        "row": int(row_index) + 2,
                    }
                )


def add_key_value_entries_from_raw(
    sheet_name: str,
    frame: "pd.DataFrame",
    kv_entries: list[dict[str, Any]],
) -> None:
    if frame.empty:
        return
    for row_index, row in frame.iterrows():
        cells = [clean_metadata_key_or_value(value) for value in row.tolist()]
        non_empty = [(index, cell) for index, cell in enumerate(cells) if cell]
        if len(non_empty) < 2:
            continue
        key_cell = non_empty[0][1]
        value_index, value_cell = non_empty[1]
        unit_cell = non_empty[2][1] if len(non_empty) > 2 else ""
        if normalize_key_for_matching(key_cell) == "value":
            continue
        kv_entries.append(
            {
                "key": key_cell,
                "normalized_key": normalize_key_for_matching(key_cell),
                "value": value_cell,
                "unit": unit_cell,
                "category": "",
                "data_type": "",
                "path": "",
                "value_decoding": metadata_cell_interpretation(value_cell),
                "sheet": sheet_name,
                "row": int(row_index) + 1,
                "column": int(value_index) + 1,
            }
        )


FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "sto_ra_distance": (
        "StoRADistance",
        "StoRA Distance",
        "SourceToRADistance",
        "Source to RA Distance",
        "Source to rotation axis distance",
        "Source-to-rotation-axis distance",
    ),
    "dto_ra_distance": (
        "DtoRADistance",
        "DtoRA Distance",
        "DetectorToRADistance",
        "Detector to RA Distance",
        "Detector to rotation axis distance",
        "Detector-to-rotation-axis distance",
        "Detector to object distance",
        "Detector-to-object distance",
        "DetectorToObjectDistance",
        "DtoObjectDistance",
        "Detector object distance",
        "Detector-to-rotation-center distance",
        "Detector to rotation center distance",
    ),
    "xray_magnification": (
        "XrayMagnification",
        "X-ray Magnification",
        "X ray geometric magnification",
        "Geometric magnification",
        "Magnification",
    ),
    "effective_pixel_size": (
        "EffectivePixelSize",
        "Effective pixel size",
        "ImagePixelSize",
        "Image pixel size",
        "PixelSize",
        "Pixel size",
        "VoxelSize",
    ),
    "image_width": ("ImageWidth", "Image width", "width_px", "ImageInfo/ImageWidth"),
    "image_height": ("ImageHeight", "Image height", "height_px", "ImageInfo/ImageHeight"),
    "image_dtype": ("NumPy dtype", "Data type", "dtype", "ImageInfo/DataType"),
    "center_shift": ("CenterShift", "AutoRecon/CenterShift", "AutoCenterShift", "Center shift"),
    "objective_lens": (
        "Objective / lens",
        "Objective lens",
        "Objective",
        "Lens",
        "LensName",
        "Lens name",
        "DetAssemblyInfo/LensInfo/LensName",
    ),
    "camera_binning": (
        "CameraBinning",
        "Camera binning",
        "Binning",
        "ImageInfo/CameraBinning",
    ),
    "field_of_view_x": (
        "Field of view X",
        "FOV_X",
        "FOV X",
        "FOVX",
        "Derived/FOV_X",
    ),
    "field_of_view_y": (
        "Field of view Y",
        "FOV_Y",
        "FOV Y",
        "FOVY",
        "Derived/FOV_Y",
    ),
    "temperature": (
        "Temperature",
        "Camera temperature",
        "Detector temperature",
        "Source temperature",
        "Stage temperature",
        "ImageInfo/Temperature",
    ),
    "cone_angle": ("ConeAngle", "Cone angle", "ConeAngleDeg", "Cone angle deg"),
    "fan_angle": ("FanAngle", "Fan angle"),
    "voltage": ("Voltage", "XrayVoltage", "X-ray voltage", "X ray voltage", "ImageInfo/Voltage"),
    "current": ("Current", "XrayCurrent", "X-ray current", "X ray current"),
    "exposure": ("Exposure", "Exposure time", "ExposureTime", "Exposure Time", "ExpTime", "ExpTimes", "Exposure_s"),
    "original_ref_corrected": ("OriginalDataRefCorrected", "Original data ref corrected"),
    "reference_data": ("ReferenceData", "Reference data", "Reference filename", "ReferenceFile"),
    "images_averaged": ("NoOfImagesAveraged", "Images averaged", "Number of images averaged"),
    "number_of_projections": ("NoOfImages", "Number of images", "Number of projections"),
}


def metadata_match_score(normalized_key: str, aliases: Iterable[str]) -> int:
    best = 0
    for alias in aliases:
        normalized_alias = normalize_key_for_matching(alias)
        if not normalized_alias:
            continue
        if normalized_key == normalized_alias:
            best = max(best, 100)
        elif normalized_key.endswith(normalized_alias):
            best = max(best, 85)
        elif normalized_alias in normalized_key:
            best = max(best, 60)
    return best


def find_metadata_entry(
    kv_entries: list[dict[str, Any]],
    field_name: str,
    preferred_categories: Iterable[str] = (),
) -> Optional[dict[str, Any]]:
    aliases = FIELD_ALIASES[field_name]
    preferred = {normalize_key_for_matching(category) for category in preferred_categories}
    best_entry: Optional[dict[str, Any]] = None
    best_score = 0
    for entry in kv_entries:
        score = metadata_match_score(entry.get("normalized_key", ""), aliases)
        if not score:
            continue
        if preferred and normalize_key_for_matching(entry.get("category", "")) in preferred:
            score += 10
        normalized_path = normalize_key_for_matching(entry.get("path", ""))
        if "imageinfo" in normalized_path:
            score += 18
        if "projectiongeometry" in normalize_key_for_matching(entry.get("sheet", "")):
            score += 12
        if "autorecon" in normalized_path or "reconsettings" in normalized_path or "reconinput" in normalized_path:
            score -= 20
        if "configurebackup" in normalized_path or "reference" in normalized_path or "multireferencedata" in normalized_path:
            score -= 12
        if parse_numeric_value(entry.get("value")) is not None:
            score += 3
        if score > best_score:
            best_score = score
            best_entry = entry
    return best_entry


def metadata_number(
    kv_entries: list[dict[str, Any]],
    field_name: str,
    preferred_categories: Iterable[str] = (),
) -> tuple[Optional[float], Optional[dict[str, Any]]]:
    entry = find_metadata_entry(kv_entries, field_name, preferred_categories)
    if entry is None:
        return None, None
    return parse_numeric_value(entry.get("value")), entry


def infer_image_info_from_frames(
    header_frames: list[tuple[str, "pd.DataFrame"]],
) -> tuple[Optional[int], Optional[int], str]:
    width: Optional[int] = None
    height: Optional[int] = None
    dtype = ""
    for sheet_name, frame in header_frames:
        if frame.empty:
            continue
        width_col = find_column(frame, ("width_px", "image_width", "image width", "width"))
        height_col = find_column(frame, ("height_px", "image_height", "image height", "height"))
        dtype_col = find_column(frame, ("dtype", "data_type", "data type", "numpy dtype"))
        role_col = find_column(frame, ("role", "classification"))
        if width_col is None or height_col is None:
            continue
        candidate_rows = frame
        if role_col is not None:
            role_mask = frame[role_col].map(lambda value: "projection" in normalize_key_for_matching(value))
            if role_mask.any():
                candidate_rows = frame[role_mask]
        for _, row in candidate_rows.iterrows():
            candidate_width = parse_numeric_value(row.get(width_col))
            candidate_height = parse_numeric_value(row.get(height_col))
            if candidate_width and candidate_height:
                width = int(candidate_width)
                height = int(candidate_height)
                if dtype_col is not None:
                    dtype = parse_dtype_value(row.get(dtype_col))
                return width, height, dtype
    return width, height, dtype


def pixel_size_to_um(
    value: Optional[float],
    unit_text: str,
    key_text: str,
    value_text: Any,
    warnings: list[str],
) -> Optional[float]:
    if value is None:
        return None
    normalized_unit = normalize_key_for_matching(unit_text)
    normalized_key = normalize_key_for_matching(key_text)
    normalized_value = normalize_key_for_matching(value_text)
    unit_haystack = f"{normalized_unit} {normalized_key} {normalized_value}"
    if "mm" in unit_haystack and "um" not in unit_haystack:
        return value * 1000.0
    if any(token in unit_haystack for token in ("um", "micrometer", "micron")):
        return value
    if any(token in normalized_unit for token in ("um", "micrometer", "micron", "µm")):
        return value
    if any(token in normalized_key for token in ("um", "micrometer", "micron")):
        return value
    if "mm" in normalized_key:
        return value * 1000.0
    if abs(value) < 1.0:
        warnings.append("Effective pixel size had no unit; interpreted value below 1 as millimeters.")
        return value * 1000.0
    warnings.append("Effective pixel size had no unit; interpreted value as micrometers.")
    return value


def projection_table_from_frames(
    header_frames: list[tuple[str, "pd.DataFrame"]],
    warnings: list[str],
) -> tuple["pd.DataFrame", dict[str, Any]]:
    best: Optional[tuple[int, str, "pd.DataFrame", dict[str, Any]]] = None
    for sheet_name, frame in header_frames:
        if frame.empty:
            continue
        filename_col = find_column(frame, ("filename", "output_name", "file", "tiff", "projection_filename"))
        index_col = find_column(frame, ("index", "projection", "zeiss_index_1based", "image_number"))
        angle_rad_col = find_column(frame, ("angle_rad", "theta_rad", "radian"))
        angle_deg_col = find_column(frame, ("angle_deg", "theta_deg", "projection_angle_deg"))
        generic_angle_col = None
        if angle_deg_col is None and angle_rad_col is None:
            generic_angle_col = find_column(frame, ("angle", "theta", "projection angle"))
        x_shift_col = find_column(frame, ("x_shift_px", "x shift", "xshift", "alignment x shift"))
        y_shift_col = find_column(frame, ("y_shift_px", "y shift", "yshift", "alignment y shift"))
        score = 0
        score += 4 if angle_deg_col is not None or angle_rad_col is not None or generic_angle_col is not None else 0
        score += 2 if filename_col is not None else 0
        score += 1 if index_col is not None else 0
        score += 1 if x_shift_col is not None or y_shift_col is not None else 0
        if "projection" in normalize_key_for_matching(sheet_name):
            score += 2
        if score >= 4 and (angle_deg_col is not None or angle_rad_col is not None or generic_angle_col is not None):
            columns = {
                "filename": filename_col,
                "index": index_col,
                "angle_deg": angle_deg_col,
                "angle_rad": angle_rad_col,
                "generic_angle": generic_angle_col,
                "x_shift_px": x_shift_col,
                "y_shift_px": y_shift_col,
            }
            if best is None or score > best[0]:
                best = (score, sheet_name, frame, columns)

    if best is None:
        warnings.append("Projection geometry table was not found.")
        return pd.DataFrame(columns=("index", "filename", "angle_deg", "angle_rad", "x_shift_px", "y_shift_px")), {}

    _, sheet_name, frame, columns = best
    rows: list[dict[str, Any]] = []
    angle_units_ambiguous = columns["generic_angle"] is not None
    if angle_units_ambiguous:
        warnings.append("Projection angle units were ambiguous; generic angle column was interpreted as degrees.")
    for _, row in frame.iterrows():
        filename = clean_metadata_key_or_value(row.get(columns["filename"])) if columns["filename"] is not None else ""
        source_index = parse_numeric_value(row.get(columns["index"])) if columns["index"] is not None else None
        angle_deg = parse_numeric_value(row.get(columns["angle_deg"])) if columns["angle_deg"] is not None else None
        angle_rad = parse_numeric_value(row.get(columns["angle_rad"])) if columns["angle_rad"] is not None else None
        if angle_deg is None and columns["generic_angle"] is not None:
            angle_deg = parse_numeric_value(row.get(columns["generic_angle"]))
        if angle_deg is None and angle_rad is not None:
            angle_deg = float(np.rad2deg(angle_rad))
        if angle_rad is None and angle_deg is not None:
            angle_rad = float(np.deg2rad(angle_deg))
        if angle_deg is None and angle_rad is None and not filename:
            continue
        if not filename and source_index is not None:
            filename = f"proj_{int(source_index):06d}.tif"
        x_shift = parse_numeric_value(row.get(columns["x_shift_px"])) if columns["x_shift_px"] is not None else None
        y_shift = parse_numeric_value(row.get(columns["y_shift_px"])) if columns["y_shift_px"] is not None else None
        rows.append(
            {
                "index": len(rows) + 1,
                "filename": filename,
                "angle_deg": angle_deg,
                "angle_rad": angle_rad,
                "x_shift_px": 0.0 if x_shift is None else x_shift,
                "y_shift_px": 0.0 if y_shift is None else y_shift,
            }
        )

    table = pd.DataFrame(rows, columns=("index", "filename", "angle_deg", "angle_rad", "x_shift_px", "y_shift_px"))
    sources = {
        "sheet": sheet_name,
        "columns": {key: clean_metadata_key_or_value(value) for key, value in columns.items() if value is not None},
    }
    return table, sources


def angle_summary_from_table(table: "pd.DataFrame", warnings: list[str]) -> dict[str, Any]:
    angles = [parse_numeric_value(value) for value in table.get("angle_deg", [])]
    angle_values = np.array([value for value in angles if value is not None], dtype=np.float64)
    if angle_values.size == 0:
        warnings.append("Projection angles are missing.")
        return {
            "angle_min_deg": None,
            "angle_max_deg": None,
            "angle_span_deg": None,
            "median_step_deg": None,
            "mean_step_deg": None,
            "ascending": None,
            "duplicate_angular_endpoint_detected": False,
            "recommend_remove_duplicate_endpoint_for_fdk_debugging": False,
        }
    diffs = np.diff(angle_values)
    median_step = float(np.median(diffs)) if diffs.size else None
    mean_step = float(np.mean(diffs)) if diffs.size else None
    ascending = bool(median_step is None or median_step >= 0)
    angle_min = float(np.min(angle_values))
    angle_max = float(np.max(angle_values))
    angle_span = angle_max - angle_min
    tolerance = 0.25 * abs(median_step) if median_step not in (None, 0) else 0.25
    endpoint_span = float(angle_values[-1] - angle_values[0])
    modular_separation = abs(((endpoint_span + 180.0) % 360.0) - 180.0)
    duplicate_endpoint = bool(abs(abs(endpoint_span) - 360.0) < tolerance or modular_separation < tolerance)
    if duplicate_endpoint:
        warnings.append("Duplicate angular endpoint detected; FDK debugging may benefit from removing one endpoint view.")
    if diffs.size:
        if ascending and np.any(diffs < -tolerance):
            warnings.append("Projection angles are non-monotonic.")
        if not ascending and np.any(diffs > tolerance):
            warnings.append("Projection angles are non-monotonic.")
    return {
        "angle_min_deg": angle_min,
        "angle_max_deg": angle_max,
        "angle_span_deg": float(angle_span),
        "median_step_deg": median_step,
        "mean_step_deg": mean_step,
        "ascending": ascending,
        "duplicate_angular_endpoint_detected": duplicate_endpoint,
        "recommend_remove_duplicate_endpoint_for_fdk_debugging": duplicate_endpoint,
    }


def projection_shift_summary(table: "pd.DataFrame", warnings: list[str]) -> dict[str, Any]:
    if table.empty or "x_shift_px" not in table.columns or "y_shift_px" not in table.columns:
        return {"present": False, "units": "pixels", "note": "No per-projection shifts found."}
    x_values = np.array([parse_numeric_value(value) or 0.0 for value in table["x_shift_px"]], dtype=np.float64)
    y_values = np.array([parse_numeric_value(value) or 0.0 for value in table["y_shift_px"]], dtype=np.float64)
    present = bool(np.any(np.abs(x_values) > 1e-12) or np.any(np.abs(y_values) > 1e-12))
    if present:
        warnings.append("Per-projection shifts were exported; reconstruction sign convention must be tested.")
    return {
        "present": present,
        "units": "pixels",
        "x_shift_px_min": float(np.min(x_values)) if x_values.size else None,
        "x_shift_px_max": float(np.max(x_values)) if x_values.size else None,
        "x_shift_px_mean": float(np.mean(x_values)) if x_values.size else None,
        "y_shift_px_min": float(np.min(y_values)) if y_values.size else None,
        "y_shift_px_max": float(np.max(y_values)) if y_values.size else None,
        "y_shift_px_mean": float(np.mean(y_values)) if y_values.size else None,
        "note": "Sign convention must be tested during reconstruction.",
    }


def compact_raw_metadata(kv_entries: list[dict[str, Any]]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for entry in kv_entries:
        key = clean_metadata_key_or_value(entry.get("key"))
        if not key:
            continue
        value = clean_metadata_key_or_value(entry.get("value"))
        if key not in compact:
            compact[key] = value
            continue
        duplicate_key = f"{key} [{entry.get('sheet', 'sheet')} row {entry.get('row', '?')}]"
        compact[duplicate_key] = value
    return compact


def compact_decoded_metadata(kv_entries: list[dict[str, Any]]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for entry in kv_entries:
        key = clean_metadata_key_or_value(entry.get("key"))
        if not key:
            continue
        decoded = entry.get("value_decoding") or metadata_cell_interpretation(entry.get("value"))
        item = {
            "cleaned_value": clean_metadata_key_or_value(entry.get("value")),
            "original_value_repr": decoded.get("original_repr"),
            "repaired_text": decoded.get("repaired_text"),
            "numeric_from_text": decoded.get("numeric_from_text"),
            "selected_binary_numeric": decoded.get("selected_binary_numeric"),
            "selected_binary_float32_series_first": decoded.get("selected_binary_float32_series_first"),
            "binary_numeric_candidates": decoded.get("binary_numeric_candidates"),
            "binary_float32_series_candidates": decoded.get("binary_float32_series_candidates"),
            "source": {
                "sheet": entry.get("sheet"),
                "row": entry.get("row"),
                "column": entry.get("column"),
                "unit": entry.get("unit"),
            },
        }
        if key not in compact:
            compact[key] = item
            continue
        duplicate_key = f"{key} [{entry.get('sheet', 'sheet')} row {entry.get('row', '?')}]"
        compact[duplicate_key] = item
    return compact


def metadata_source(entry: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if entry is None:
        return None
    return {
        "sheet": entry.get("sheet"),
        "row": entry.get("row"),
        "column": entry.get("column"),
        "key": entry.get("key"),
        "path": entry.get("path"),
        "unit": entry.get("unit"),
    }


def metadata_text_value(
    kv_entries: list[dict[str, Any]],
    field_name: str,
    preferred_categories: Iterable[str] = (),
) -> dict[str, Any]:
    entry = find_metadata_entry(kv_entries, field_name, preferred_categories)
    if entry is None:
        return {"present": False, "value": None, "source": None}
    return {
        "present": True,
        "value": clean_metadata_key_or_value(entry.get("value")),
        "source": metadata_source(entry),
        "decoding": entry.get("value_decoding") or metadata_cell_interpretation(entry.get("value")),
    }


def metadata_numeric_value(
    kv_entries: list[dict[str, Any]],
    field_name: str,
    preferred_categories: Iterable[str] = (),
) -> dict[str, Any]:
    entry = find_metadata_entry(kv_entries, field_name, preferred_categories)
    value = parse_numeric_value(entry.get("value")) if entry is not None else None
    return {
        "present": entry is not None,
        "value": value,
        "unit": clean_metadata_key_or_value(entry.get("unit")) if entry is not None else "",
        "source": metadata_source(entry),
        "decoding": (entry.get("value_decoding") or metadata_cell_interpretation(entry.get("value"))) if entry else None,
    }


def field_of_view_value_to_um(
    value: Optional[float],
    unit_text: str,
    key_text: str,
    value_text: Any,
    warnings: list[str],
) -> Optional[float]:
    if value is None:
        return None
    normalized = f"{normalize_key_for_matching(unit_text)} {normalize_key_for_matching(key_text)} {normalize_key_for_matching(value_text)}"
    if "mm" in normalized and "um" not in normalized:
        return value * 1000.0
    if any(token in normalized for token in ("um", "micrometer", "micron")):
        return value
    warnings.append("Field-of-view value had no unit; interpreted it as micrometers.")
    return value


def build_scan_parameters(
    kv_entries: list[dict[str, Any]],
    projection_table: "pd.DataFrame",
    projection_acquisition_stats: dict[str, dict[str, Any]],
    image_width: Optional[int],
    image_height: Optional[int],
    effective_pixel_size_um: Optional[float],
    xray_magnification: Optional[float],
    xray_mag_metadata: Optional[float],
    xray_mag_from_distances: Optional[float],
    cone_angle: Optional[float],
    cone_entry: Optional[dict[str, Any]],
    fan_angle: Optional[float],
    fan_entry: Optional[dict[str, Any]],
    warnings: list[str],
) -> dict[str, Any]:
    objective = metadata_text_value(kv_entries, "objective_lens", ("Microscope",))
    exposure = projection_acquisition_stats.get("exposure_time") or projection_numeric_stat(
        projection_table,
        "exposure_s",
        "s",
    ) or first_series_stat(
        kv_entries,
        "exposure",
    )
    voltage = projection_acquisition_stats.get("voltage") or projection_numeric_stat(
        projection_table,
        "voltage_kV",
        "kV",
    ) or first_series_stat(
        kv_entries,
        "voltage",
    )
    current = projection_acquisition_stats.get("current") or projection_numeric_stat(
        projection_table,
        "current_uA",
        "uA",
    ) or first_series_stat(
        kv_entries,
        "current",
    )
    camera_binning = metadata_numeric_value(kv_entries, "camera_binning", ("Image",))
    temperature = best_plausible_series_stat(
        kv_entries,
        "temperature",
        minimum=-120.0,
        maximum=120.0,
        preferred_path_tokens=("imageinfo/cameratemperature", "cameratemperature", "temperatureinfo/temperatures"),
    )

    fov_x_entry = find_metadata_entry(kv_entries, "field_of_view_x", ("Image", "Geometry"))
    fov_y_entry = find_metadata_entry(kv_entries, "field_of_view_y", ("Image", "Geometry"))
    fov_x_raw = parse_numeric_value(fov_x_entry.get("value")) if fov_x_entry is not None else None
    fov_y_raw = parse_numeric_value(fov_y_entry.get("value")) if fov_y_entry is not None else None
    fov_x_um = (
        field_of_view_value_to_um(fov_x_raw, fov_x_entry.get("unit", ""), fov_x_entry.get("key", ""), fov_x_entry.get("value"), warnings)
        if fov_x_entry is not None
        else None
    )
    fov_y_um = (
        field_of_view_value_to_um(fov_y_raw, fov_y_entry.get("unit", ""), fov_y_entry.get("key", ""), fov_y_entry.get("value"), warnings)
        if fov_y_entry is not None
        else None
    )
    computed_fov_x_um = image_width * effective_pixel_size_um if image_width and effective_pixel_size_um is not None else None
    computed_fov_y_um = image_height * effective_pixel_size_um if image_height and effective_pixel_size_um is not None else None
    if fov_x_um is None:
        fov_x_um = computed_fov_x_um
    if fov_y_um is None:
        fov_y_um = computed_fov_y_um

    return {
        "xray_magnification": {
            "value": xray_magnification,
            "metadata_value": xray_mag_metadata,
            "from_distances": xray_mag_from_distances,
            "source": metadata_source(find_metadata_entry(kv_entries, "xray_magnification")),
        },
        "objective_lens": objective,
        "exposure_time": exposure,
        "voltage": voltage,
        "current": current,
        "camera_binning": camera_binning,
        "field_of_view": {
            "x_um": fov_x_um,
            "y_um": fov_y_um,
            "x_mm": fov_x_um * 1e-3 if fov_x_um is not None else None,
            "y_mm": fov_y_um * 1e-3 if fov_y_um is not None else None,
            "computed_x_um": computed_fov_x_um,
            "computed_y_um": computed_fov_y_um,
            "source_x": metadata_source(fov_x_entry),
            "source_y": metadata_source(fov_y_entry),
            "formula_if_computed": "FOV_um = image_pixels * effective_pixel_size_um",
        },
        "temperature": temperature,
        "cone_angle": {
            "value": cone_angle,
            "unit": clean_metadata_key_or_value(cone_entry.get("unit")) if cone_entry else "",
            "source": metadata_source(cone_entry),
            "note": "Cone angle is stored as metadata only; TIGRE geometry uses DSO, DSD, and detector size.",
        },
        "fan_angle": {
            "value": fan_angle,
            "unit": clean_metadata_key_or_value(fan_entry.get("unit")) if fan_entry else "",
            "source": metadata_source(fan_entry),
        },
    }


def summarize_numeric_values(
    values: Iterable[Any],
    source: Optional[dict[str, Any]] = None,
    unit: str = "",
) -> Optional[dict[str, Any]]:
    parsed = [parse_numeric_value(value) for value in values]
    clean_values = [value for value in parsed if value is not None and math.isfinite(value)]
    if not clean_values:
        return None
    arr = np.array(clean_values, dtype=np.float64)
    return {
        "present": True,
        "source": source,
        "unit": unit,
        "first": float(arr[0]),
        "min": float(np.min(arr)),
        "max": float(np.max(arr)),
        "mean": float(np.mean(arr)),
        "count": int(arr.size),
    }


def projection_numeric_stat(frame: "pd.DataFrame", column: str, unit: str) -> Optional[dict[str, Any]]:
    if frame.empty or column not in frame.columns:
        return None
    summary = summarize_numeric_values(
        frame[column].tolist(),
        source={"sheet": "ProjectionGeometry", "column": column},
        unit=unit,
    )
    if summary is not None:
        summary["preferred_source_note"] = "ProjectionGeometry per-projection values were preferred."
    return summary


def projection_geometry_acquisition_stats(
    header_frames: list[tuple[str, "pd.DataFrame"]],
    projection_sources: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    source_sheet = projection_sources.get("sheet")
    if not source_sheet:
        return {}
    frame = next((candidate for sheet, candidate in header_frames if sheet == source_sheet), pd.DataFrame())
    if frame.empty:
        return {}
    columns = {
        "exposure_time": (find_column(frame, ("exposure_s", "exposure", "exposure_time", "exptime")), "s"),
        "current": (find_column(frame, ("current_uA", "current", "xray_current", "x-ray current")), "uA"),
        "voltage": (find_column(frame, ("voltage_kV", "voltage", "xray_voltage", "x-ray voltage")), "kV"),
    }
    stats: dict[str, dict[str, Any]] = {}
    for name, (column, unit) in columns.items():
        if column is None:
            continue
        summary = summarize_numeric_values(
            frame[column].tolist(),
            source={"sheet": source_sheet, "column": clean_metadata_key_or_value(column)},
            unit=unit,
        )
        if summary is not None:
            summary["preferred_source_note"] = "ProjectionGeometry per-projection values were preferred."
            stats[name] = summary
    return stats


def best_plausible_series_stat(
    kv_entries: list[dict[str, Any]],
    field_name: str,
    minimum: float,
    maximum: float,
    preferred_path_tokens: Iterable[str] = (),
) -> dict[str, Any]:
    aliases = FIELD_ALIASES[field_name]
    preferred_tokens = [normalize_key_for_matching(token) for token in preferred_path_tokens]
    best_entry: Optional[dict[str, Any]] = None
    best_values: list[float] = []
    best_score = -10_000
    for entry in kv_entries:
        base_score = metadata_match_score(entry.get("normalized_key", ""), aliases)
        if not base_score:
            continue
        values = parse_numeric_series(entry.get("value"))
        plausible_values = [
            value
            for value in values
            if value is not None and math.isfinite(value) and minimum <= value <= maximum
        ]
        if not plausible_values:
            continue
        max_abs = max(abs(value) for value in plausible_values)
        normalized_path = normalize_key_for_matching(entry.get("path", ""))
        score = base_score + min(30, len(plausible_values))
        if max_abs < 1e-6:
            score -= 120
        if any(token and token in normalized_path for token in preferred_tokens):
            score += 80
        if "imageinfo" in normalized_path:
            score += 20
        if "detassemblyinfo" in normalized_path and "camera" not in normalized_path:
            score -= 30
        if "alignment" in normalized_path:
            score -= 40
        if score > best_score:
            best_score = score
            best_entry = entry
            best_values = plausible_values
    if best_entry is None:
        return {"present": False, "source": None, "note": "No physically plausible value found."}
    summary = summarize_numeric_values(best_values, metadata_source(best_entry), clean_metadata_key_or_value(best_entry.get("unit")))
    assert summary is not None
    return summary


def first_series_stat(kv_entries: list[dict[str, Any]], field_name: str) -> dict[str, Any]:
    entry = find_metadata_entry(kv_entries, field_name)
    if entry is None:
        return {"present": False}
    values = parse_numeric_series(entry.get("value"))
    if not values:
        number = parse_numeric_value(entry.get("value"))
        values = [] if number is None else [number]
    result: dict[str, Any] = {
        "present": True,
        "source": {
            "sheet": entry.get("sheet"),
            "row": entry.get("row"),
            "key": entry.get("key"),
            "unit": entry.get("unit"),
        },
    }
    if values:
        arr = np.array(values, dtype=np.float64)
        result.update(
            {
                "first": float(arr[0]),
                "min": float(np.min(arr)),
                "max": float(np.max(arr)),
                "mean": float(np.mean(arr)),
                "count": int(arr.size),
            }
        )
    else:
        result["value"] = clean_metadata_key_or_value(entry.get("value"))
    return result


def build_geometry_summary_text(geometry: dict[str, Any]) -> str:
    projection_data = geometry["projection_data"]
    angles = geometry["angles"]
    distances = geometry["distances"]
    pixel_size = geometry["pixel_size"]
    tigre = geometry["tigre_geometry"]
    center_shift = geometry["center_shift"]
    shifts = geometry["projection_shifts"]
    reference = geometry["reference_correction"]
    scan = geometry.get("scan_parameters", {})
    warnings_list = geometry.get("warnings", [])

    def fmt(value: Any, suffix: str = "") -> str:
        if value is None:
            return "missing"
        if isinstance(value, float):
            return f"{value:.9g}{suffix}"
        return f"{value}{suffix}"

    lines = [
        "Geometry Summary",
        "================",
        "",
        "Input metadata file:",
        str(geometry["source_metadata_file"]),
        "",
        "Projection data:",
        f"- Number of projections: {fmt(projection_data.get('num_projections'))}",
        f"- Image size: {fmt(projection_data.get('image_width_px'))} x {fmt(projection_data.get('image_height_px'))} px",
        f"- Data type: {projection_data.get('dtype') or 'missing'}",
        "",
        "Angles:",
        f"- Angle range: {fmt(angles.get('angle_min_deg'))} to {fmt(angles.get('angle_max_deg'))} deg",
        f"- Median angular step: {fmt(angles.get('median_step_deg'))} deg",
        "- Converted to radians for TIGRE",
        f"- Duplicate endpoint detected: {'yes' if angles.get('duplicate_angular_endpoint_detected') else 'no'}",
        "",
        "Distances:",
        f"- DSO = abs(StoRADistance) = {fmt(distances.get('DSO_mm'))} mm",
        f"- Detector-to-object/RA distance = abs(DtoRADistance) = {fmt(distances.get('DOD_mm'))} mm",
        f"- DSD = DSO + DOD = {fmt(distances.get('DSD_mm'))} mm",
        f"- X-ray magnification = DSD / DSO = {fmt(distances.get('xray_magnification_from_distances'))}",
        f"- Distance convention: {distances.get('distance_convention_note')}",
        "",
        "Pixel size:",
        f"- Effective object-plane pixel size = {fmt(pixel_size.get('effective_pixel_size_um'))} um",
        f"- TIGRE detector pixel size = effective_pixel_size * DSD / DSO = {fmt(pixel_size.get('dDetector_mm'))} mm",
        "- Do not use the object-plane pixel size directly as geo.dDetector.",
        "",
        "Scan metadata:",
        f"- XrayMagnification = {fmt((scan.get('xray_magnification') or {}).get('value'))}",
        f"- Objective / lens = {(scan.get('objective_lens') or {}).get('value') or 'missing'}",
        f"- Exposure time = {fmt((scan.get('exposure_time') or {}).get('first'))} s",
        f"- Voltage = {fmt((scan.get('voltage') or {}).get('first'))} kV",
        f"- Camera binning = {fmt((scan.get('camera_binning') or {}).get('value'))}",
        f"- Field of view = {fmt((scan.get('field_of_view') or {}).get('x_um'))} x {fmt((scan.get('field_of_view') or {}).get('y_um'))} um",
        f"- Temperature = {fmt((scan.get('temperature') or {}).get('first'))}",
        f"- Cone angle = {fmt((scan.get('cone_angle') or {}).get('value'))}",
        "",
        "TIGRE geometry:",
        f"- geo.DSO = {fmt(tigre.get('geo_DSO'))}",
        f"- geo.DSD = {fmt(tigre.get('geo_DSD'))}",
        f"- geo.nDetector = {tigre.get('geo_nDetector')}",
        f"- geo.dDetector = {tigre.get('geo_dDetector')}",
        f"- geo.sDetector = {tigre.get('geo_sDetector')}",
        f"- recommended geo.dVoxel = {tigre.get('geo_dVoxel_full')}",
        f"- recommended full geo.nVoxel = {tigre.get('geo_nVoxel_full')}",
        f"- recommended full geo.sVoxel = {tigre.get('geo_sVoxel_full')}",
        "",
        "Center shift:",
        f"- CenterShift = {fmt(center_shift.get('center_shift_px'))} px",
        f"- Equivalent detector offset = {fmt(center_shift.get('center_shift_mm'))} mm",
        "- Test both signs in TIGRE offDetector.",
        "",
        "Projection shifts:",
        f"- x_shift_px present: {'yes' if shifts.get('present') else 'no'}",
        f"- y_shift_px present: {'yes' if shifts.get('present') else 'no'}",
        f"- x shift range: {fmt(shifts.get('x_shift_px_min'))} to {fmt(shifts.get('x_shift_px_max'))}",
        f"- y shift range: {fmt(shifts.get('y_shift_px_min'))} to {fmt(shifts.get('y_shift_px_max'))}",
        "- These shifts may need to be applied before FDK or encoded as per-projection detector offsets.",
        "",
        "Preprocessing:",
        f"- OriginalDataRefCorrected = {fmt(reference.get('original_data_ref_corrected'))}",
        f"- Recommended preprocessing: {reference.get('recommended_preprocessing')}",
        "",
        "Warnings:",
    ]
    lines.extend([f"- {warning}" for warning in warnings_list] or ["- none"])
    lines.extend(
        [
            "",
            "Reconstruction-ready Python snippet:",
            "```python",
            "import json",
            "import numpy as np",
            "import pandas as pd",
            "import tigre",
            "",
            'with open("tigre_fdk_geometry.json", "r") as f:',
            "    g = json.load(f)",
            "",
            'geo = tigre.geometry(mode="cone", default=False)',
            'tg = g["tigre_geometry"]',
            "",
            'geo.DSO = tg["geo_DSO"]',
            'geo.DSD = tg["geo_DSD"]',
            'geo.nDetector = np.array(tg["geo_nDetector"])',
            'geo.dDetector = np.array(tg["geo_dDetector"])',
            'geo.sDetector = np.array(tg["geo_sDetector"])',
            'geo.nVoxel = np.array(tg["geo_nVoxel_full"])',
            'geo.dVoxel = np.array(tg["geo_dVoxel_full"])',
            'geo.sVoxel = np.array(tg["geo_sVoxel_full"])',
            'geo.offOrigin = np.array(tg["geo_offOrigin_default"])',
            'geo.offDetector = np.array(tg["geo_offDetector_default"])',
            "",
            'proj_table = pd.read_csv("projection_angle_table.csv")',
            'angles_rad = proj_table["angle_rad"].to_numpy(dtype=np.float32)',
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def extract_tigre_fdk_geometry(metadata_xlsx_path: Path | str, output_dir: Path | str) -> dict[str, Any]:
    """
    Extract TIGRE FDK reconstruction geometry from a Zeiss/Xradia TXRM metadata workbook.

    Writes TIGRE geometry JSON, summary text, projection table CSV, and a cleaned
    metadata dump into output_dir.
    """
    if missing_dependency is not None:
        raise RuntimeError(dependency_message())
    metadata_path = Path(metadata_xlsx_path)
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata workbook not found: {metadata_path}")
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    kv_entries, cleaned_dump, header_frames, warnings = read_metadata_workbook(metadata_path)
    projection_table, projection_sources = projection_table_from_frames(header_frames, warnings)
    projection_acquisition_stats = projection_geometry_acquisition_stats(header_frames, projection_sources)
    width_from_frames, height_from_frames, dtype_from_frames = infer_image_info_from_frames(header_frames)
    metadata_projection_count, metadata_projection_count_entry = metadata_number(
        kv_entries,
        "number_of_projections",
        ("Image",),
    )
    decoded_binary_entries = [
        entry
        for entry in kv_entries
        if (entry.get("value_decoding") or {}).get("binary_numeric_candidates")
    ]
    if decoded_binary_entries:
        warnings.append(
            f"{len(decoded_binary_entries)} key-value metadata entries had byte-like values; "
            "numeric decode candidates were saved in raw_decoded_metadata."
        )

    width_value, width_entry = metadata_number(kv_entries, "image_width", ("Image",))
    height_value, height_entry = metadata_number(kv_entries, "image_height", ("Image",))
    dtype_entry = find_metadata_entry(kv_entries, "image_dtype", ("Image",))
    image_width = int(width_value) if width_value is not None else width_from_frames
    image_height = int(height_value) if height_value is not None else height_from_frames
    dtype = parse_dtype_value(dtype_entry.get("value")) if dtype_entry is not None else dtype_from_frames

    sto_ra, sto_entry = metadata_number(kv_entries, "sto_ra_distance", ("Geometry", "Metadata"))
    dto_ra, dto_entry = metadata_number(kv_entries, "dto_ra_distance", ("Geometry", "Metadata"))
    dso_mm = abs(sto_ra) if sto_ra is not None else None
    dod_mm = abs(dto_ra) if dto_ra is not None else None
    dsd_mm = dso_mm + dod_mm if dso_mm is not None and dod_mm is not None else None
    if dso_mm is None:
        warnings.append("DSO is missing: StoRADistance/SourceToRADistance was not found.")
    if dod_mm is None:
        warnings.append("DOD is missing: DtoRADistance/DetectorToRADistance was not found.")
    if dsd_mm is None:
        warnings.append("DSD cannot be computed because DSO or DOD is missing.")

    xray_mag_metadata, xray_entry = metadata_number(kv_entries, "xray_magnification")
    xray_mag_from_distances = dsd_mm / dso_mm if dso_mm not in (None, 0) and dsd_mm is not None else None
    xray_magnification = xray_mag_metadata if xray_mag_metadata is not None else xray_mag_from_distances
    if xray_mag_metadata is not None and xray_mag_from_distances is not None and xray_mag_metadata != 0:
        relative_error = abs(xray_mag_from_distances - xray_mag_metadata) / abs(xray_mag_metadata)
        if relative_error > 0.01:
            warnings.append(
                "XrayMagnification differs from DSD/DSO by more than 1% "
                f"({relative_error * 100:.3g}%)."
            )
    else:
        relative_error = None

    pixel_size_raw, pixel_entry = metadata_number(kv_entries, "effective_pixel_size", ("Image", "Geometry"))
    pixel_key = pixel_entry.get("key", "") if pixel_entry else ""
    pixel_unit = pixel_entry.get("unit", "") if pixel_entry else ""
    effective_pixel_size_um = pixel_size_to_um(
        pixel_size_raw,
        pixel_unit,
        pixel_key,
        pixel_entry.get("value") if pixel_entry else "",
        warnings,
    )
    effective_pixel_size_mm = effective_pixel_size_um * 1e-3 if effective_pixel_size_um is not None else None
    if effective_pixel_size_um is None:
        warnings.append("Effective pixel size is missing.")
    d_detector_mm = (
        effective_pixel_size_mm * dsd_mm / dso_mm
        if effective_pixel_size_mm is not None and dsd_mm is not None and dso_mm not in (None, 0)
        else None
    )

    if image_width is None or image_height is None:
        warnings.append("Image size is missing.")
    n_detector = [image_height, image_width] if image_width is not None and image_height is not None else None
    d_detector = [d_detector_mm, d_detector_mm] if d_detector_mm is not None else None
    s_detector = (
        [image_height * d_detector_mm, image_width * d_detector_mm]
        if image_width is not None and image_height is not None and d_detector_mm is not None
        else None
    )

    if image_width is not None and image_height is not None:
        n_voxel_full = [image_height, image_width, image_width]
        n_voxel_debug = [min(512, value) for value in n_voxel_full]
    else:
        n_voxel_full = None
        n_voxel_debug = None
    d_voxel = [effective_pixel_size_mm] * 3 if effective_pixel_size_mm is not None else None
    s_voxel_full = (
        [value * effective_pixel_size_mm for value in n_voxel_full]
        if n_voxel_full is not None and effective_pixel_size_mm is not None
        else None
    )
    s_voxel_debug = (
        [value * effective_pixel_size_mm for value in n_voxel_debug]
        if n_voxel_debug is not None and effective_pixel_size_mm is not None
        else None
    )
    d_voxel_debug_binned = (
        [s_voxel_full[index] / n_voxel_debug[index] for index in range(3)]
        if s_voxel_full is not None and n_voxel_debug is not None
        else None
    )

    center_shift_px, center_entry = metadata_number(kv_entries, "center_shift")
    center_shift_mm = center_shift_px * d_detector_mm if center_shift_px is not None and d_detector_mm is not None else None
    center_shift = {
        "center_shift_px": center_shift_px,
        "center_shift_mm": center_shift_mm,
        "candidate_offDetector_mm_positive": [0.0, center_shift_mm] if center_shift_mm is not None else None,
        "candidate_offDetector_mm_negative": [0.0, -center_shift_mm] if center_shift_mm is not None else None,
        "source": {
            "sheet": center_entry.get("sheet"),
            "row": center_entry.get("row"),
            "key": center_entry.get("key"),
        }
        if center_entry is not None
        else None,
    }

    angle_summary = angle_summary_from_table(projection_table, warnings)
    shift_summary = projection_shift_summary(projection_table, warnings)
    filenames = [clean_metadata_key_or_value(value) for value in projection_table.get("filename", [])]
    angle_count = int(projection_table["angle_deg"].notna().sum()) if "angle_deg" in projection_table else 0
    filename_count = len([name for name in filenames if name])
    projection_count = int(len(projection_table))
    expected_projection_count = int(metadata_projection_count) if metadata_projection_count is not None else None
    if projection_count == 0 and expected_projection_count is not None:
        projection_count = expected_projection_count
    if expected_projection_count is not None and len(projection_table) and expected_projection_count != len(projection_table):
        warnings.append(
            f"ImageInfo/NoOfImages reports {expected_projection_count} projections, "
            f"but the projection table has {len(projection_table)} rows."
        )
    if filename_count == 0:
        warnings.append("Projection filenames are missing.")
    if angle_count and filename_count and angle_count != filename_count:
        warnings.append("Number of angles does not equal number of projection filenames.")
    duplicate_filenames = len(filenames) != len(set(filenames)) if filenames else False
    if duplicate_filenames:
        warnings.append("Projection table has duplicate filenames.")
    if not projection_table.empty and {"x_shift_px", "y_shift_px"}.issubset(projection_table.columns):
        pass
    elif not projection_table.empty:
        warnings.append("Per-projection shifts are absent from the projection table.")

    cone_angle, cone_entry = metadata_number(kv_entries, "cone_angle")
    fan_angle, fan_entry = metadata_number(kv_entries, "fan_angle")
    if cone_angle is not None or fan_angle is not None:
        warnings.append(
            "Cone/Fan angle found; extractor does not assume half-angle or full-angle unless the key says so."
        )
    scan_parameters = build_scan_parameters(
        kv_entries,
        projection_table,
        projection_acquisition_stats,
        image_width,
        image_height,
        effective_pixel_size_um,
        xray_magnification,
        xray_mag_metadata,
        xray_mag_from_distances,
        cone_angle,
        cone_entry,
        fan_angle,
        fan_entry,
        warnings,
    )

    original_ref_corrected, ref_corrected_entry = metadata_number(kv_entries, "original_ref_corrected")
    reference_entry = find_metadata_entry(kv_entries, "reference_data")
    images_averaged, images_averaged_entry = metadata_number(kv_entries, "images_averaged")
    if original_ref_corrected == 1:
        preprocessing = "Projection data may already be reference-corrected. Avoid applying flat-field correction twice unless confirmed."
    else:
        preprocessing = "T = I / F, p = -ln(T), because no dark current is available unless supplied separately."

    table_csv_path = output_path / GEOMETRY_TABLE_NAME
    projection_table.to_csv(table_csv_path, index=False)
    cleaned_dump_path = output_path / GEOMETRY_CLEANED_DUMP_NAME
    with cleaned_dump_path.open("w", encoding="utf-8") as handle:
        json.dump(json_safe(cleaned_dump), handle, indent=2, ensure_ascii=True)

    geometry = {
        "source_metadata_file": str(metadata_path),
        "created_by": "txrm_read.py::extract_tigre_fdk_geometry",
        "units": {
            "distances": "mm",
            "angles": "radians for TIGRE, degrees also stored",
            "pixel_size": "mm for TIGRE geometry, um also stored for user display",
        },
        "output_files": {
            "geometry_json": GEOMETRY_JSON_NAME,
            "geometry_summary": GEOMETRY_SUMMARY_NAME,
            "projection_table_csv": GEOMETRY_TABLE_NAME,
            "cleaned_metadata_dump_json": GEOMETRY_CLEANED_DUMP_NAME,
        },
        "extraction_sources": {
            "projection_table": projection_sources,
            "DSO": {"sheet": sto_entry.get("sheet"), "row": sto_entry.get("row"), "key": sto_entry.get("key")}
            if sto_entry
            else None,
            "DOD": {"sheet": dto_entry.get("sheet"), "row": dto_entry.get("row"), "key": dto_entry.get("key")}
            if dto_entry
            else None,
            "pixel_size": {
                "sheet": pixel_entry.get("sheet"),
                "row": pixel_entry.get("row"),
                "key": pixel_entry.get("key"),
                "unit": pixel_entry.get("unit"),
            }
            if pixel_entry
            else None,
            "number_of_projections": {
                "sheet": metadata_projection_count_entry.get("sheet"),
                "row": metadata_projection_count_entry.get("row"),
                "key": metadata_projection_count_entry.get("key"),
            }
            if metadata_projection_count_entry
            else None,
        },
        "projection_data": {
            "num_projections": projection_count,
            "metadata_num_projections": expected_projection_count,
            "image_height_px": image_height,
            "image_width_px": image_width,
            "dtype": dtype,
            "projection_table_csv": GEOMETRY_TABLE_NAME,
            "projection_filenames": filenames,
        },
        "angles": angle_summary,
        "distances": {
            "DSO_mm": dso_mm,
            "DOD_mm": dod_mm,
            "DSD_mm": dsd_mm,
            "StoRADistance_mm": sto_ra,
            "DtoRADistance_mm": dto_ra,
            "source_to_object_distance_mm": dso_mm,
            "source_to_rotation_axis_distance_mm": dso_mm,
            "detector_to_object_distance_mm": dod_mm,
            "detector_to_rotation_axis_distance_mm": dod_mm,
            "distance_convention_note": (
                "DtoRADistance is treated as detector-to-object/rotation-axis distance. "
                "TIGRE geo.DSD is computed as abs(StoRADistance) + abs(DtoRADistance)."
            ),
            "xray_magnification": xray_magnification,
            "xray_magnification_metadata": xray_mag_metadata,
            "xray_magnification_from_distances": xray_mag_from_distances,
            "xray_magnification_relative_error": relative_error,
        },
        "pixel_size": {
            "effective_pixel_size_um": effective_pixel_size_um,
            "effective_pixel_size_mm": effective_pixel_size_mm,
            "dDetector_mm": d_detector_mm,
            "dDetector_pair_mm": d_detector,
            "detector_pixel_size_formula": "dDetector_mm = effective_pixel_size_mm * DSD_mm / DSO_mm",
        },
        "tigre_geometry": {
            "geo_DSO": dso_mm,
            "geo_DSD": dsd_mm,
            "geo_nDetector": n_detector,
            "geo_dDetector": d_detector,
            "geo_sDetector": s_detector,
            "geo_nVoxel_full": n_voxel_full,
            "geo_dVoxel_full": d_voxel,
            "geo_sVoxel_full": s_voxel_full,
            "geo_nVoxel_debug": n_voxel_debug,
            "geo_dVoxel_debug": d_voxel,
            "geo_sVoxel_debug": s_voxel_debug,
            "geo_nVoxel_debug_binned": n_voxel_debug,
            "geo_dVoxel_debug_binned": d_voxel_debug_binned,
            "geo_sVoxel_debug_binned": s_voxel_full,
            "geo_offOrigin_default": [0.0, 0.0, 0.0],
            "geo_offDetector_default": [0.0, 0.0],
        },
        "center_shift": center_shift,
        "projection_shifts": shift_summary,
        "cone_fan_angle": {
            "cone_angle": cone_angle,
            "fan_angle": fan_angle,
            "note": (
                "Cone/Fan angle found in metadata. The extractor does not assume whether this is "
                "half-angle or full-angle unless the key explicitly says so."
            )
            if cone_angle is not None or fan_angle is not None
            else "",
            "sources": {
                "cone_angle": {"sheet": cone_entry.get("sheet"), "row": cone_entry.get("row"), "key": cone_entry.get("key")}
                if cone_entry
                else None,
                "fan_angle": {"sheet": fan_entry.get("sheet"), "row": fan_entry.get("row"), "key": fan_entry.get("key")}
                if fan_entry
                else None,
            },
        },
        "acquisition": {
            "voltage": scan_parameters.get("voltage"),
            "current": scan_parameters.get("current"),
            "exposure": scan_parameters.get("exposure_time"),
        },
        "scan_parameters": scan_parameters,
        "reference_correction": {
            "original_data_ref_corrected": original_ref_corrected,
            "reference_data_present": reference_entry is not None,
            "reference_filename": clean_metadata_key_or_value(reference_entry.get("value")) if reference_entry else "",
            "number_of_images_averaged": images_averaged,
            "recommended_preprocessing": preprocessing,
            "sources": {
                "original_data_ref_corrected": {
                    "sheet": ref_corrected_entry.get("sheet"),
                    "row": ref_corrected_entry.get("row"),
                    "key": ref_corrected_entry.get("key"),
                }
                if ref_corrected_entry
                else None,
                "number_of_images_averaged": {
                    "sheet": images_averaged_entry.get("sheet"),
                    "row": images_averaged_entry.get("row"),
                    "key": images_averaged_entry.get("key"),
                }
                if images_averaged_entry
                else None,
            },
        },
        "warnings": warnings,
        "raw_cleaned_metadata": compact_raw_metadata(kv_entries),
        "raw_decoded_metadata": compact_decoded_metadata(kv_entries),
        "metadata_decoding_note": (
            "Cleaned strings are used for matching, but original representations, repaired mojibake, "
            "and byte-level numeric candidates are preserved so unusual Zeiss metadata characters are not discarded."
        ),
    }

    geometry_json_path = output_path / GEOMETRY_JSON_NAME
    summary_path = output_path / GEOMETRY_SUMMARY_NAME
    geometry["output_paths"] = {
        "output_dir": str(output_path),
        "geometry_json": str(geometry_json_path),
        "geometry_json_alias": str(output_path / GEOMETRY_JSON_ALIAS_NAME),
        "geometry_summary": str(summary_path),
        "geometry_summary_alias": str(output_path / GEOMETRY_SUMMARY_ALIAS_NAME),
        "projection_table_csv": str(table_csv_path),
        "cleaned_metadata_dump_json": str(cleaned_dump_path),
    }
    with geometry_json_path.open("w", encoding="utf-8") as handle:
        json.dump(json_safe(geometry), handle, indent=2, ensure_ascii=True)
    with (output_path / GEOMETRY_JSON_ALIAS_NAME).open("w", encoding="utf-8") as handle:
        json.dump(json_safe(geometry), handle, indent=2, ensure_ascii=True)

    summary_text = build_geometry_summary_text(geometry)
    summary_path.write_text(summary_text, encoding="utf-8")
    (output_path / GEOMETRY_SUMMARY_ALIAS_NAME).write_text(summary_text, encoding="utf-8")
    return geometry


def scan_geometry_from_extract_dir(
    extract_dir: Path,
    progress: Optional[Callable[[int, str], None]] = None,
    logger: Optional[logging.Logger] = None,
) -> dict[str, Any]:
    extract_dir = Path(extract_dir)
    metadata_path = extract_dir / "metadata" / "metadata.xlsx"
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata workbook not found: {metadata_path}")
    output_dir = extract_dir / "metadata" / GEOMETRY_SCAN_FOLDER
    if progress is not None:
        progress(10, "Reading metadata workbook")
    geometry = extract_tigre_fdk_geometry(metadata_path, output_dir)
    if progress is not None:
        progress(100, "Geometry scan complete")
    if logger is not None:
        logger.info("Geometry scan written to %s", output_dir)
        for warning in geometry.get("warnings", []):
            logger.warning("Geometry scan: %s", warning)
    return {
        "output_dir": output_dir,
        "geometry_json": output_dir / GEOMETRY_JSON_NAME,
        "geometry_summary": output_dir / GEOMETRY_SUMMARY_NAME,
        "projection_table_csv": output_dir / GEOMETRY_TABLE_NAME,
        "cleaned_metadata_dump": output_dir / GEOMETRY_CLEANED_DUMP_NAME,
        "num_projections": geometry.get("projection_data", {}).get("num_projections"),
        "warnings": geometry.get("warnings", []),
    }


def average_reference_images(
    reference_files: list[Path],
    progress: Optional[Callable[[int, str], None]] = None,
) -> "np.ndarray":
    if not reference_files:
        raise ValueError("No reference TIFF images were found in the reference folder.")
    reference_sum: Optional[np.ndarray] = None
    for index, file_path in enumerate(reference_files, start=1):
        image = tifffile.imread(str(file_path)).astype(np.float64, copy=False)
        if reference_sum is None:
            reference_sum = np.zeros_like(image, dtype=np.float64)
        if image.shape != reference_sum.shape:
            raise ValueError(
                f"Reference image shape mismatch: {file_path.name} has {image.shape}, "
                f"expected {reference_sum.shape}."
            )
        reference_sum += image
        if progress is not None:
            progress(int(20 * index / len(reference_files)), f"Averaging reference {index}/{len(reference_files)}")
    assert reference_sum is not None
    return (reference_sum / len(reference_files)).astype(np.float32)


def flatfield_correct_folder(
    extract_dir: Path,
    progress: Optional[Callable[[int, str], None]] = None,
    logger: Optional[logging.Logger] = None,
    eps: float = 1e-6,
    limit: Optional[int] = None,
) -> dict[str, Any]:
    extract_dir = Path(extract_dir)
    projection_files = list_tiff_files(extract_dir / "projections")
    reference_files = list_tiff_files(extract_dir / "reference")
    if not projection_files:
        raise ValueError(f"No projection TIFF images found in {extract_dir / 'projections'}")
    reference_mean = average_reference_images(reference_files, progress)
    output_dir = extract_dir / "flatfield_corrected"
    output_dir.mkdir(parents=True, exist_ok=True)
    tifffile.imwrite(str(output_dir / "reference_average_float32.tif"), reference_mean)

    files_to_process = projection_files[:limit] if limit is not None else projection_files
    total = len(files_to_process)
    denominator = np.maximum(reference_mean.astype(np.float32, copy=False), np.float32(eps))
    for index, projection_path in enumerate(files_to_process, start=1):
        projection = tifffile.imread(str(projection_path)).astype(np.float32, copy=False)
        if projection.shape != denominator.shape:
            raise ValueError(
                f"Projection image shape mismatch: {projection_path.name} has {projection.shape}, "
                f"reference average has {denominator.shape}."
            )
        corrected = np.maximum(projection, np.float32(eps)) / denominator
        output_path = output_dir / projection_output_name(projection_path, "flatfield")
        tifffile.imwrite(str(output_path), corrected.astype(np.float32, copy=False))
        if logger is not None:
            logger.info("Flat-field corrected %s -> %s", projection_path.name, output_path.name)
        if progress is not None:
            percent = 20 + int(80 * index / max(total, 1))
            progress(percent, f"Flat-field corrected {index}/{total}")
    return {
        "output_dir": output_dir,
        "count": total,
        "reference_count": len(reference_files),
        "reference_average": output_dir / "reference_average_float32.tif",
    }


def attenuation_convert_folder(
    extract_dir: Path,
    progress: Optional[Callable[[int, str], None]] = None,
    logger: Optional[logging.Logger] = None,
    eps: float = 1e-6,
    limit: Optional[int] = None,
) -> dict[str, Any]:
    extract_dir = Path(extract_dir)
    flatfield_files = [
        path
        for path in list_tiff_files(extract_dir / "flatfield_corrected")
        if not path.name.lower().startswith("reference_average")
    ]
    if not flatfield_files:
        raise ValueError(
            f"No flat-field corrected TIFF images found in {extract_dir / 'flatfield_corrected'}"
        )
    output_dir = extract_dir / "attenuation"
    output_dir.mkdir(parents=True, exist_ok=True)
    files_to_process = flatfield_files[:limit] if limit is not None else flatfield_files
    total = len(files_to_process)
    for index, flatfield_path in enumerate(files_to_process, start=1):
        corrected = tifffile.imread(str(flatfield_path)).astype(np.float32, copy=False)
        attenuation = -np.log(np.maximum(corrected, np.float32(eps))).astype(np.float32, copy=False)
        output_path = output_dir / projection_output_name(flatfield_path, "attenuation")
        tifffile.imwrite(str(output_path), attenuation)
        if logger is not None:
            logger.info("Converted attenuation %s -> %s", flatfield_path.name, output_path.name)
        if progress is not None:
            progress(int(100 * index / max(total, 1)), f"Converted attenuation {index}/{total}")
    return {"output_dir": output_dir, "count": total}


def configure_processing_logger(extract_dir: Path) -> logging.Logger:
    logger = logging.getLogger("txrm_processing")
    logger.setLevel(logging.INFO)
    log_dir = Path(extract_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_dir / "preprocessing.log", mode="a", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.handlers.clear()
    logger.addHandler(handler)
    return logger


class TXRMExporter:
    def __init__(
        self,
        txrm_file: Path,
        output_dir: Optional[Path] = None,
        progress: Optional[Callable[[int, str], None]] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.txrm_file = Path(txrm_file)
        self.output_dir = Path(output_dir) if output_dir else self.default_output_dir()
        self.progress = progress or (lambda percent, message: None)
        self.logger = logger or logging.getLogger("txrm_export")
        self.preview_first: Optional[np.ndarray] = None
        self.preview_middle: Optional[np.ndarray] = None
        self.reader: Optional[TXRMReader] = None

    def default_output_dir(self) -> Path:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self.txrm_file.with_name(f"{self.txrm_file.stem}_txrm_export_{stamp}")

    def run(self) -> dict[str, Any]:
        self._prepare_output()
        self._configure_file_logging()
        self.logger.info("Starting TXRM export: %s", self.txrm_file)
        self.progress(2, "Opening TXRM container")
        with TXRMReader(self.txrm_file) as reader:
            self.reader = reader
            self._validate_reader(reader)
            self.progress(8, "Writing metadata tables")
            self._write_coordinate_note(reader)
            metadata_xlsx = self._write_metadata(reader)
            counts = self._export_images(reader)
        self.progress(100, "Export complete")
        self.logger.info("Export complete: %s", self.output_dir)
        return {
            "output_dir": self.output_dir,
            "metadata_xlsx": metadata_xlsx,
            "log_file": self.output_dir / "logs" / "txrm_export.log",
            "counts": counts,
            "reader": self.reader,
            "preview_first": self.preview_first,
            "preview_middle": self.preview_middle,
        }

    def _prepare_output(self) -> None:
        for folder in ("projections", "reference", "dark_field", "auxiliary_images", "metadata", "logs"):
            (self.output_dir / folder).mkdir(parents=True, exist_ok=True)

    def _configure_file_logging(self) -> None:
        self.logger.setLevel(logging.INFO)
        log_path = self.output_dir / "logs" / "txrm_export.log"
        handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        self.logger.handlers.clear()
        self.logger.addHandler(handler)

    def _validate_reader(self, reader: TXRMReader) -> None:
        projections = [stream for stream in reader.image_streams if stream.role == "projection"]
        expected = reader.metadata.get("number_of_images")
        if expected and len(projections) != expected:
            self.logger.warning(
                "ImageInfo/NoOfImages reports %s projections, discovered %s projection streams.",
                expected,
                len(projections),
            )
        if not projections:
            raise ValueError("No projection streams matching ImageData#/Image# were found.")
        for stream in projections[:5]:
            expected_size = stream.width * stream.height * np.dtype(stream.dtype_name).itemsize
            if stream.size_bytes != expected_size:
                raise ValueError(
                    f"{stream.path} has {stream.size_bytes} bytes, expected {expected_size}."
                )
        self.logger.info("Discovered %s projections.", len(projections))

    def _write_metadata(self, reader: TXRMReader) -> Path:
        metadata_dir = self.output_dir / "metadata"
        metadata_xlsx = metadata_dir / "metadata.xlsx"

        metadata_rows = [
            {
                "category": excel_safe_text(entry.category),
                "path": excel_safe_text(entry.path),
                "parameter": excel_safe_text(entry.parameter),
                "value": safe_sheet_value(entry.value),
                "unit": excel_safe_text(entry.unit),
                "data_type": excel_safe_text(entry.data_type),
                "count": entry.count,
                "notes": excel_safe_text(entry.notes),
            }
            for entry in reader.entries
        ]
        streams_rows = [
            {
                "path": excel_safe_text(path),
                "size_bytes": reader.stream_size(path),
                "classification": self._stream_classification(reader, path),
            }
            for path in reader.stream_paths
        ]
        geometry_rows = reader.projection_geometry_rows()
        images_rows = [
            {
                "role": excel_safe_text(stream.role),
                "stream_path": excel_safe_text(stream.path),
                "zeiss_index_1based": stream.index_1based,
                "output_name": excel_safe_text(stream.output_name),
                "dtype": excel_safe_text(stream.dtype_name),
                "width_px": stream.width,
                "height_px": stream.height,
                "size_bytes": stream.size_bytes,
            }
            for stream in reader.image_streams
        ]

        metadata_df = sanitize_dataframe_for_excel(pd.DataFrame(metadata_rows))
        geometry_df = sanitize_dataframe_for_excel(pd.DataFrame(geometry_rows))
        images_df = sanitize_dataframe_for_excel(pd.DataFrame(images_rows))
        streams_df = sanitize_dataframe_for_excel(pd.DataFrame(streams_rows))

        with pd.ExcelWriter(metadata_xlsx, engine="openpyxl") as writer:
            metadata_df.to_excel(writer, index=False, sheet_name="Metadata")
            geometry_df.to_excel(writer, index=False, sheet_name="ProjectionGeometry")
            images_df.to_excel(writer, index=False, sheet_name="ImageStreams")
            streams_df.to_excel(writer, index=False, sheet_name="AllOLEStreams")

        geometry_df.to_csv(metadata_dir / "projection_geometry.csv", index=False)
        with (metadata_dir / "metadata.json").open("w", encoding="utf-8") as handle:
            json.dump(metadata_rows, handle, indent=2, ensure_ascii=True)
        self.logger.info("Metadata written: %s", metadata_xlsx)
        return metadata_xlsx

    def _write_coordinate_note(self, reader: TXRMReader) -> None:
        note = {
            "image_array_order": "native TXRM row-major order, shape=(height, width)",
            "image_x_axis": "TIFF columns correspond to detector/image X pixels",
            "image_y_axis": "TIFF rows correspond to detector/image Y pixels",
            "projection_order": "proj_000001.tif corresponds to ZEISS Image1, then Image2, ...",
            "angles": "ImageInfo/Angles exported in degrees and radians; no sign change applied",
            "stage_positions": "ImageInfo/XPosition, YPosition, ZPosition exported as stored; units are vendor metadata units when not explicitly labeled",
            "alignment": "alignment/x-shifts and y-shifts exported as stored in pixels",
            "astra_note": "Use metadata/projection_geometry.csv or Metadata.xlsx ProjectionGeometry sheet to feed ASTRA angle and shift arrays.",
        }
        with (self.output_dir / "metadata" / "coordinate_system_notes.json").open("w", encoding="utf-8") as handle:
            json.dump(note, handle, indent=2)
        reader._add_entry(
            "Geometry",
            "Export/CoordinateSystemNotesFile",
            "Coordinate notes file",
            "metadata/coordinate_system_notes.json",
            "",
            "export",
        )

    def _stream_classification(self, reader: TXRMReader, path: str) -> str:
        for stream in reader.image_streams:
            if stream.path == path:
                return stream.role
        if path_key(path) in reader.entry_by_key:
            return "metadata"
        return "unknown"

    def _export_images(self, reader: TXRMReader) -> dict[str, int]:
        role_dirs = {
            "projection": self.output_dir / "projections",
            "reference": self.output_dir / "reference",
            "dark_field": self.output_dir / "dark_field",
            "auxiliary": self.output_dir / "auxiliary_images",
        }
        counts = {role: 0 for role in role_dirs}
        total = len(reader.image_streams)
        projections = [stream for stream in reader.image_streams if stream.role == "projection"]
        middle_seq = len(projections) // 2

        for seq, stream in enumerate(reader.image_streams, start=1):
            array = reader.read_image(stream)
            folder = role_dirs.get(stream.role, role_dirs["auxiliary"])
            output_path = folder / stream.output_name
            tifffile.imwrite(str(output_path), array, photometric="minisblack")
            counts[stream.role if stream.role in counts else "auxiliary"] += 1
            self.logger.info("Wrote %s -> %s", stream.path, output_path)

            if stream.role == "projection":
                projection_seq = counts["projection"] - 1
                if projection_seq == 0:
                    self.preview_first = array.copy()
                if projection_seq == middle_seq:
                    self.preview_middle = array.copy()

            percent = 10 + int(85 * seq / max(total, 1))
            self.progress(percent, f"Exported {seq}/{total}: {stream.output_name}")
        return counts


def array_to_display_uint8(array: "np.ndarray") -> "np.ndarray":
    data = np.asarray(array)
    if data.size == 0:
        return np.zeros((1, 1), dtype=np.uint8)
    if data.dtype.kind == "b":
        return data.astype(np.uint8) * 255

    numeric = np.abs(data) if data.dtype.kind == "c" else data
    numeric = numeric.astype(np.float64, copy=False)
    finite = numeric[np.isfinite(numeric)]
    if not finite.size:
        return np.zeros(data.shape, dtype=np.uint8)

    low, high = np.percentile(finite, PREVIEW_CONTRAST_PERCENTILES)
    if high <= low:
        low = float(finite.min())
        high = float(finite.max())
    if high <= low:
        fill = 0 if low == 0 else 128
        display = np.zeros(data.shape, dtype=np.uint8)
        display[np.isfinite(numeric)] = fill
        return display

    scaled = np.clip((numeric - low) / (high - low), 0, 1) * 255
    scaled = np.nan_to_num(scaled, nan=0.0, posinf=255.0, neginf=0.0)
    display = scaled.astype(np.uint8)
    return display


def array_to_photo(array: "np.ndarray", max_size: tuple[int, int] = (360, 280)) -> "ImageTk.PhotoImage":
    display = array_to_display_uint8(array)
    image = Image.fromarray(display)
    image.thumbnail(max_size, Image.Resampling.LANCZOS)
    return ImageTk.PhotoImage(image)


class TXRMApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1120x760")
        self.minsize(960, 640)
        self.result_queue: "queue.Queue[tuple[str, Any]]" = queue.Queue()
        self.worker: Optional[threading.Thread] = None
        self.action_buttons: list[ttk.Button] = []
        self.first_photo: Optional[ImageTk.PhotoImage] = None
        self.middle_photo: Optional[ImageTk.PhotoImage] = None
        self.content_pane: Optional[ttk.PanedWindow] = None
        self.preview_root_dir: Optional[Path] = None
        self.preview_source_var = tk.StringVar(value=PREVIEW_SOURCE_EXTRACTED)
        self._configure_theme()
        self._build_ui()
        self.after(100, self._poll_queue)

    def _configure_theme(self) -> None:
        self.configure(background=APP_BACKGROUND)
        self.option_add("*TCombobox*Listbox.background", APP_FIELD_BACKGROUND)
        self.option_add("*TCombobox*Listbox.foreground", APP_TEXT)
        self.option_add("*TCombobox*Listbox.selectBackground", APP_SELECTION)
        self.option_add("*TCombobox*Listbox.selectForeground", APP_TEXT)
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        style.configure(
            ".",
            background=APP_BACKGROUND,
            foreground=APP_TEXT,
            fieldbackground=APP_FIELD_BACKGROUND,
            bordercolor=APP_BORDER,
            lightcolor=APP_BACKGROUND,
            darkcolor=APP_BACKGROUND,
            troughcolor=APP_FIELD_BACKGROUND,
            arrowcolor=APP_TEXT,
        )
        style.map(".", foreground=[("disabled", APP_MUTED_TEXT)])

        style.configure("TFrame", background=APP_BACKGROUND)
        style.configure("TPanedwindow", background=APP_BACKGROUND)
        style.configure("Sash", background=APP_BORDER)
        style.configure("TLabel", background=APP_BACKGROUND, foreground=APP_TEXT)
        style.configure(
            "TLabelframe",
            background=APP_BACKGROUND,
            foreground=APP_TEXT,
            bordercolor=APP_BORDER,
            relief="solid",
        )
        style.configure("TLabelframe.Label", background=APP_BACKGROUND, foreground=APP_TEXT)
        style.configure(
            "TButton",
            background=APP_BUTTON_BACKGROUND,
            foreground=APP_TEXT,
            bordercolor=APP_BORDER,
            focusthickness=1,
            focuscolor=APP_BORDER,
            padding=(10, 4),
        )
        style.map(
            "TButton",
            background=[("active", APP_BUTTON_ACTIVE), ("pressed", APP_FIELD_BACKGROUND), ("disabled", APP_SURFACE)],
            foreground=[("disabled", APP_MUTED_TEXT)],
        )
        style.configure(
            "TEntry",
            fieldbackground=APP_FIELD_BACKGROUND,
            foreground=APP_TEXT,
            insertcolor=APP_TEXT,
            bordercolor=APP_BORDER,
            lightcolor=APP_BORDER,
            darkcolor=APP_BORDER,
        )
        style.map(
            "TEntry",
            fieldbackground=[("disabled", APP_SURFACE), ("readonly", APP_FIELD_BACKGROUND)],
            foreground=[("disabled", APP_MUTED_TEXT), ("readonly", APP_TEXT)],
        )
        style.configure(
            "TCombobox",
            fieldbackground=APP_FIELD_BACKGROUND,
            background=APP_BUTTON_BACKGROUND,
            foreground=APP_TEXT,
            arrowcolor=APP_TEXT,
            bordercolor=APP_BORDER,
            selectbackground=APP_SELECTION,
            selectforeground=APP_TEXT,
        )
        style.map(
            "TCombobox",
            fieldbackground=[("readonly", APP_FIELD_BACKGROUND)],
            background=[("active", APP_BUTTON_ACTIVE), ("readonly", APP_FIELD_BACKGROUND)],
            foreground=[("readonly", APP_TEXT), ("disabled", APP_MUTED_TEXT)],
            arrowcolor=[("disabled", APP_MUTED_TEXT)],
        )
        style.configure("TNotebook", background=APP_BACKGROUND, bordercolor=APP_BORDER)
        style.configure(
            "TNotebook.Tab",
            background=APP_SURFACE,
            foreground=APP_TEXT,
            padding=(12, 6),
        )
        style.map(
            "TNotebook.Tab",
            background=[("selected", APP_BUTTON_BACKGROUND), ("active", APP_BUTTON_ACTIVE)],
            foreground=[("selected", APP_TEXT), ("active", APP_TEXT)],
        )
        style.configure(
            "Treeview",
            background=APP_FIELD_BACKGROUND,
            fieldbackground=APP_FIELD_BACKGROUND,
            foreground=APP_TEXT,
            bordercolor=APP_BORDER,
            rowheight=24,
        )
        style.map("Treeview", background=[("selected", APP_SELECTION)], foreground=[("selected", APP_TEXT)])
        style.configure(
            "Treeview.Heading",
            background=APP_TREE_HEADING,
            foreground=APP_TEXT,
            bordercolor=APP_BORDER,
            relief="flat",
        )
        style.map("Treeview.Heading", background=[("active", APP_BUTTON_ACTIVE)])
        style.configure(
            "TScrollbar",
            background=APP_BUTTON_BACKGROUND,
            troughcolor=APP_FIELD_BACKGROUND,
            bordercolor=APP_BORDER,
            arrowcolor=APP_TEXT,
        )
        style.map("TScrollbar", background=[("active", APP_BUTTON_ACTIVE)])
        style.configure(
            "Horizontal.TProgressbar",
            background=APP_TEXT,
            troughcolor=APP_FIELD_BACKGROUND,
            bordercolor=APP_BORDER,
            lightcolor=APP_TEXT,
            darkcolor=APP_TEXT,
        )

    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)

        controls = ttk.Frame(self, padding=12)
        controls.grid(row=0, column=0, sticky="ew")
        controls.columnconfigure(1, weight=1)

        ttk.Label(controls, text="TXRM file").grid(row=0, column=0, sticky="w", padx=(0, 8))
        self.file_var = tk.StringVar()
        ttk.Entry(controls, textvariable=self.file_var).grid(row=0, column=1, sticky="ew")
        ttk.Button(controls, text="Browse", command=self._browse_file).grid(row=0, column=2, padx=(8, 0))

        ttk.Label(controls, text="Output folder").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=(8, 0))
        self.output_var = tk.StringVar()
        ttk.Entry(controls, textvariable=self.output_var).grid(row=1, column=1, sticky="ew", pady=(8, 0))
        ttk.Button(controls, text="Browse", command=self._browse_output).grid(row=1, column=2, padx=(8, 0), pady=(8, 0))

        action_bar = ttk.Frame(self, padding=(12, 0, 12, 8))
        action_bar.grid(row=1, column=0, sticky="ew")
        action_bar.columnconfigure(4, weight=1)
        self.start_button = ttk.Button(action_bar, text="Extract", command=self._start_export)
        self.start_button.grid(row=0, column=0, sticky="w")
        self.flatfield_button = ttk.Button(
            action_bar,
            text="Flat-field Correct",
            command=self._start_flatfield_correction,
        )
        self.flatfield_button.grid(row=0, column=1, sticky="w", padx=(8, 0))
        self.attenuation_button = ttk.Button(
            action_bar,
            text="Attenuation",
            command=self._start_attenuation_conversion,
        )
        self.attenuation_button.grid(row=0, column=2, sticky="w", padx=(8, 0))
        self.geometry_button = ttk.Button(
            action_bar,
            text="Scan Geometry",
            command=self._start_geometry_scan,
        )
        self.geometry_button.grid(row=0, column=3, sticky="w", padx=(8, 0))
        self.action_buttons = [
            self.start_button,
            self.flatfield_button,
            self.attenuation_button,
            self.geometry_button,
        ]
        self.progress = ttk.Progressbar(action_bar, mode="determinate", maximum=100)
        self.progress.grid(row=0, column=4, sticky="ew", padx=12)
        self.status_var = tk.StringVar(value="Choose a TXRM file to begin.")
        ttk.Label(action_bar, textvariable=self.status_var).grid(row=0, column=5, sticky="e")

        content = ttk.PanedWindow(self, orient=tk.HORIZONTAL)
        content.grid(row=2, column=0, sticky="nsew", padx=12, pady=(0, 12))
        self.content_pane = content

        left = ttk.Frame(content)
        left.rowconfigure(0, weight=1)
        left.rowconfigure(1, weight=1)
        left.columnconfigure(0, weight=1)
        right = ttk.Notebook(content)
        content.add(left, weight=4)
        content.add(right, weight=1)

        preview_frame = ttk.LabelFrame(left, text="Projection Preview", padding=10)
        preview_frame.grid(row=0, column=0, sticky="nsew", pady=(0, 8))
        preview_frame.columnconfigure(0, weight=1)
        preview_frame.columnconfigure(1, weight=1)
        source_bar = ttk.Frame(preview_frame)
        source_bar.grid(row=0, column=0, columnspan=2, sticky="ew")
        source_bar.columnconfigure(1, weight=1)
        ttk.Label(source_bar, text="Source").grid(row=0, column=0, sticky="w", padx=(0, 8))
        preview_source_combo = ttk.Combobox(
            source_bar,
            textvariable=self.preview_source_var,
            values=PREVIEW_SOURCES,
            state="readonly",
        )
        preview_source_combo.grid(row=0, column=1, sticky="ew")
        preview_source_combo.bind("<<ComboboxSelected>>", self._handle_preview_source_changed)
        ttk.Label(preview_frame, text="Start projection").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Label(preview_frame, text="Middle projection").grid(row=1, column=1, sticky="w", pady=(8, 0))
        self.first_label = ttk.Label(preview_frame, text="No image yet", anchor="center")
        self.middle_label = ttk.Label(preview_frame, text="No image yet", anchor="center")
        self.first_label.grid(row=2, column=0, sticky="nsew", padx=(0, 8), pady=(8, 0))
        self.middle_label.grid(row=2, column=1, sticky="nsew", padx=(8, 0), pady=(8, 0))
        preview_frame.rowconfigure(2, weight=1)

        shift_frame = ttk.LabelFrame(left, text="Image Shift vs Projection Angle", padding=8)
        shift_frame.grid(row=1, column=0, sticky="nsew")
        shift_frame.rowconfigure(0, weight=1)
        shift_frame.columnconfigure(0, weight=1)
        self.shift_figure = Figure(figsize=(5.2, 3.4), dpi=100, facecolor=APP_BACKGROUND)
        self.shift_x_axis = self.shift_figure.add_subplot(211)
        self.shift_y_axis = self.shift_figure.add_subplot(212)
        self.shift_figure.tight_layout(pad=1.4)
        self.shift_canvas = FigureCanvasTkAgg(self.shift_figure, master=shift_frame)
        self.shift_canvas.get_tk_widget().configure(background=APP_BACKGROUND, highlightthickness=0)
        self.shift_canvas.get_tk_widget().grid(row=0, column=0, sticky="nsew")
        self._clear_shift_plots()

        metadata_tab = ttk.Frame(right, padding=8)
        log_tab = ttk.Frame(right, padding=8)
        right.add(metadata_tab, text="Metadata")
        right.add(log_tab, text="Log")

        metadata_tab.rowconfigure(0, weight=1)
        metadata_tab.columnconfigure(0, weight=1)
        self.metadata_tree = ttk.Treeview(
            metadata_tab,
            columns=("category", "parameter", "value", "unit", "path"),
            show="headings",
            height=20,
        )
        for col, width in (
            ("category", 120),
            ("parameter", 180),
            ("value", 300),
            ("unit", 70),
            ("path", 260),
        ):
            self.metadata_tree.heading(col, text=col.replace("_", " ").title())
            self.metadata_tree.column(col, width=width, anchor="w")
        self.metadata_tree.grid(row=0, column=0, sticky="nsew")
        scroll = ttk.Scrollbar(metadata_tab, orient=tk.VERTICAL, command=self.metadata_tree.yview)
        scroll.grid(row=0, column=1, sticky="ns")
        x_scroll = ttk.Scrollbar(metadata_tab, orient=tk.HORIZONTAL, command=self.metadata_tree.xview)
        x_scroll.grid(row=1, column=0, sticky="ew")
        self.metadata_tree.configure(yscrollcommand=scroll.set, xscrollcommand=x_scroll.set)

        log_tab.rowconfigure(0, weight=1)
        log_tab.columnconfigure(0, weight=1)
        self.log_text = tk.Text(
            log_tab,
            wrap="word",
            height=20,
            background=APP_FIELD_BACKGROUND,
            foreground=APP_TEXT,
            insertbackground=APP_TEXT,
            selectbackground=APP_SELECTION,
            selectforeground=APP_TEXT,
            relief="flat",
            highlightthickness=1,
            highlightbackground=APP_BORDER,
            highlightcolor=APP_BORDER,
        )
        self.log_text.grid(row=0, column=0, sticky="nsew")
        log_scroll = ttk.Scrollbar(log_tab, orient=tk.VERTICAL, command=self.log_text.yview)
        log_scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scroll.set)

        self.after_idle(self._set_initial_panel_sizes)

    def _set_initial_panel_sizes(self) -> None:
        if self.content_pane is None:
            return
        self.update_idletasks()
        width = self.content_pane.winfo_width()
        if width <= 1:
            self.after(50, self._set_initial_panel_sizes)
            return
        left_width = int(width * (1.0 - METADATA_PANEL_FRACTION))
        try:
            self.content_pane.sashpos(0, left_width)
        except tk.TclError:
            pass

    def _handle_preview_source_changed(self, event: Any = None) -> None:
        self._refresh_projection_preview()

    def _show_preview_message(self, message: str) -> None:
        self.first_photo = None
        self.middle_photo = None
        self.first_label.configure(image="", text=message)
        self.middle_label.configure(image="", text=message)

    def _refresh_projection_preview(self, extract_dir: Optional[Path] = None) -> None:
        if extract_dir is not None:
            self.preview_root_dir = Path(extract_dir)
        elif self.output_var.get().strip():
            output_path = Path(self.output_var.get().strip())
            if self.preview_root_dir is None or output_path != self.preview_root_dir:
                self.preview_root_dir = output_path

        if self.preview_root_dir is None:
            self._show_preview_message("No image yet")
            return
        if missing_dependency is not None:
            self._show_preview_message("Preview unavailable")
            return

        source = self.preview_source_var.get() or PREVIEW_SOURCE_EXTRACTED
        files = preview_files_for_source(self.preview_root_dir, source)
        if not files:
            self._show_preview_message(f"No {source.lower()} images")
            return

        first_path = files[0]
        middle_path = files[len(files) // 2]
        try:
            first_array = tifffile.imread(str(first_path))
            middle_array = tifffile.imread(str(middle_path))
            self.first_photo = array_to_photo(first_array)
            self.middle_photo = array_to_photo(middle_array)
        except Exception as exc:
            self._show_preview_message("Preview load failed")
            self._log(f"Preview load failed for {source}: {exc}")
            return

        self.first_label.configure(image=self.first_photo, text="")
        self.middle_label.configure(image=self.middle_photo, text="")
        self._log(f"Preview source: {source} ({first_path.name}, {middle_path.name})")

    def _browse_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Choose ZEISS TXRM/TXM file",
            filetypes=[("ZEISS TXRM/TXM", "*.txrm *.txm"), ("All files", "*.*")],
        )
        if path:
            self.file_var.set(path)
            self.output_var.set("")
            self.preview_root_dir = None
            self.preview_source_var.set(PREVIEW_SOURCE_EXTRACTED)
            self._show_preview_message("No image yet")

    def _browse_output(self) -> None:
        path = filedialog.askdirectory(title="Choose output folder")
        if path:
            self.output_var.set(path)
            self.preview_root_dir = Path(path)
            self._refresh_projection_preview()

    def _start_export(self) -> None:
        if missing_dependency is not None:
            messagebox.showerror("Missing dependency", dependency_message())
            return
        txrm_path = Path(self.file_var.get().strip())
        if not txrm_path.exists():
            messagebox.showerror("File not found", "Choose a valid TXRM/TXM file.")
            return
        output_dir = Path(self.output_var.get().strip()) if self.output_var.get().strip() else None
        self._set_actions_state("disabled")
        self.progress.configure(value=0)
        self._clear_metadata()
        self._clear_shift_plots()
        self._log(f"Starting export: {txrm_path}")
        self.status_var.set("Starting export")

        def progress(percent: int, message: str) -> None:
            self.result_queue.put(("progress", (percent, message)))

        def worker() -> None:
            try:
                exporter = TXRMExporter(txrm_path, output_dir=output_dir, progress=progress)
                result = exporter.run()
            except Exception:
                self.result_queue.put(("error", traceback.format_exc()))
            else:
                self.result_queue.put(("done", result))

        self.worker = threading.Thread(target=worker, daemon=True)
        self.worker.start()

    def _start_flatfield_correction(self) -> None:
        extract_dir = self._selected_extract_dir()
        if extract_dir is None:
            return
        self._start_processing_worker(
            "flatfield",
            extract_dir,
            lambda progress, logger: flatfield_correct_folder(extract_dir, progress, logger),
        )

    def _start_attenuation_conversion(self) -> None:
        extract_dir = self._selected_extract_dir()
        if extract_dir is None:
            return
        self._start_processing_worker(
            "attenuation",
            extract_dir,
            lambda progress, logger: attenuation_convert_folder(extract_dir, progress, logger),
        )

    def _start_geometry_scan(self) -> None:
        extract_dir = self._selected_extract_dir()
        if extract_dir is None:
            return
        metadata_path = extract_dir / "metadata" / "metadata.xlsx"
        if not metadata_path.exists():
            messagebox.showerror("Metadata not found", f"Could not find metadata workbook:\n{metadata_path}")
            return
        self._start_processing_worker(
            "geometry scan",
            extract_dir,
            lambda progress, logger: scan_geometry_from_extract_dir(extract_dir, progress, logger),
        )

    def _selected_extract_dir(self) -> Optional[Path]:
        output_text = self.output_var.get().strip()
        if not output_text:
            messagebox.showerror(
                "Output folder required",
                "Choose the extraction output folder first. It should contain projections/ and reference/.",
            )
            return None
        extract_dir = Path(output_text)
        if not extract_dir.exists():
            messagebox.showerror("Folder not found", f"This folder does not exist:\n{extract_dir}")
            return None
        return extract_dir

    def _start_processing_worker(
        self,
        operation: str,
        extract_dir: Path,
        processor: Callable[[Callable[[int, str], None], logging.Logger], dict[str, Any]],
    ) -> None:
        self._set_actions_state("disabled")
        self.progress.configure(value=0)
        self._log(f"Starting {operation}: {extract_dir}")
        self.status_var.set(f"Starting {operation}")

        def progress(percent: int, message: str) -> None:
            self.result_queue.put(("progress", (percent, message)))

        def worker() -> None:
            try:
                logger = configure_processing_logger(extract_dir)
                result = processor(progress, logger)
                result["operation"] = operation
                result["extract_dir"] = extract_dir
            except Exception:
                self.result_queue.put(("error", traceback.format_exc()))
            else:
                self.result_queue.put(("process_done", result))

        self.worker = threading.Thread(target=worker, daemon=True)
        self.worker.start()

    def _set_actions_state(self, state: str) -> None:
        for button in self.action_buttons:
            button.configure(state=state)

    def _poll_queue(self) -> None:
        try:
            while True:
                kind, payload = self.result_queue.get_nowait()
                if kind == "progress":
                    percent, message = payload
                    self.progress.configure(value=percent)
                    self.status_var.set(message)
                    self._log(message)
                elif kind == "done":
                    self._finish_success(payload)
                elif kind == "process_done":
                    self._finish_processing(payload)
                elif kind == "error":
                    self._finish_error(payload)
        except queue.Empty:
            pass
        self.after(100, self._poll_queue)

    def _finish_success(self, result: dict[str, Any]) -> None:
        self.progress.configure(value=100)
        counts = result["counts"]
        output_dir = result["output_dir"]
        self.status_var.set(f"Done: {output_dir}")
        self._log(f"Finished. Output folder: {output_dir}")
        self._log(f"Counts: {counts}")
        self.output_var.set(str(output_dir))
        self._set_actions_state("normal")

        reader = result.get("reader")
        if reader is not None:
            self._show_metadata(reader.entries)
            self._show_shift_plots(reader.projection_geometry_rows())
        self.preview_source_var.set(PREVIEW_SOURCE_EXTRACTED)
        self._refresh_projection_preview(Path(output_dir))

    def _finish_processing(self, result: dict[str, Any]) -> None:
        self.progress.configure(value=100)
        operation = result.get("operation", "processing")
        output_dir = result.get("output_dir")
        count = result.get("count")
        self.status_var.set(f"{operation} complete")
        if operation == "geometry scan":
            self._log(f"Geometry scan complete -> {output_dir}")
            self._log(f"Geometry JSON: {result.get('geometry_json')}")
            self._log(f"Geometry summary: {result.get('geometry_summary')}")
            self._log(f"Projection table: {result.get('projection_table_csv')}")
            if result.get("num_projections") is not None:
                self._log(f"Geometry projections: {result.get('num_projections')}")
            for warning in result.get("warnings", [])[:8]:
                self._log(f"Geometry warning: {warning}")
        else:
            self._log(f"{operation} complete: {count} images -> {output_dir}")
        if result.get("reference_count") is not None:
            self._log(
                f"Averaged {result['reference_count']} reference images: {result.get('reference_average')}"
            )
        self._set_actions_state("normal")

        preview_source_by_operation = {
            "flatfield": PREVIEW_SOURCE_FLATFIELD,
            "attenuation": PREVIEW_SOURCE_ATTENUATION,
        }
        preview_source = preview_source_by_operation.get(operation)
        extract_dir = result.get("extract_dir")
        if preview_source is not None and extract_dir is not None:
            self.preview_source_var.set(preview_source)
            self._refresh_projection_preview(Path(extract_dir))

    def _finish_error(self, error_text: str) -> None:
        self._set_actions_state("normal")
        self.status_var.set("Export failed")
        self._log(error_text)
        messagebox.showerror("Export failed", error_text.splitlines()[-1] if error_text else "Export failed")

    def _show_metadata(self, entries: list[MetadataEntry]) -> None:
        self._clear_metadata()
        priority = {"Dataset": 0, "Microscope": 1, "Image": 2, "Geometry": 3, "Alignment": 4}
        sorted_entries = sorted(entries, key=lambda e: (priority.get(e.category, 99), e.category, e.parameter))
        for entry in sorted_entries[:1000]:
            self.metadata_tree.insert(
                "",
                "end",
                values=(
                    entry.category,
                    entry.parameter,
                    safe_sheet_value(entry.value),
                    entry.unit,
                    entry.path,
                ),
            )

    def _clear_shift_plots(self) -> None:
        for axis, title in (
            (self.shift_x_axis, "X Shift"),
            (self.shift_y_axis, "Y Shift"),
        ):
            axis.clear()
            self._style_shift_axis(axis, title)
        self.shift_canvas.draw_idle()

    def _show_shift_plots(self, geometry_rows: list[dict[str, Any]]) -> None:
        self._clear_shift_plots()
        x_points = extract_shift_points(geometry_rows, "x_shift_px")
        y_points = extract_shift_points(geometry_rows, "y_shift_px")
        self._plot_shift_series(self.shift_x_axis, x_points, "X Shift", PLOT_X_COLOR)
        self._plot_shift_series(self.shift_y_axis, y_points, "Y Shift", PLOT_Y_COLOR)
        self.shift_figure.tight_layout(pad=1.4)
        self.shift_canvas.draw_idle()

    def _style_shift_axis(self, axis: Any, title: str) -> None:
        axis.set_facecolor(APP_BACKGROUND)
        axis.set_title(title, color=APP_TEXT)
        axis.set_xlabel("Projection angle (deg)", color=APP_TEXT)
        axis.set_ylabel("Pixel shift", color=APP_TEXT)
        axis.tick_params(colors=APP_MUTED_TEXT)
        axis.grid(True, color=APP_MUTED_TEXT, alpha=0.25)
        for spine in axis.spines.values():
            spine.set_color(APP_BORDER)

    def _plot_shift_series(
        self,
        axis: Any,
        points: tuple[list[float], list[float]],
        title: str,
        color: str,
    ) -> None:
        angles, shifts = points
        axis.clear()
        self._style_shift_axis(axis, title)
        if angles and shifts:
            axis.plot(angles, shifts, color=color, linewidth=1.2)
            axis.scatter(angles, shifts, color=color, s=8, alpha=0.6)
            axis.margins(x=0.02)
        else:
            axis.text(
                0.5,
                0.5,
                "No shift data",
                color=APP_MUTED_TEXT,
                ha="center",
                va="center",
                transform=axis.transAxes,
            )

    def _clear_metadata(self) -> None:
        for item in self.metadata_tree.get_children():
            self.metadata_tree.delete(item)

    def _log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert("end", f"[{timestamp}] {message}\n")
        self.log_text.see("end")


def run_cli(args: argparse.Namespace) -> int:
    if missing_dependency is not None:
        print(dependency_message(), file=sys.stderr)
        return 2
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    exporter = TXRMExporter(Path(args.input), Path(args.output) if args.output else None)
    result = exporter.run()
    print(f"Output folder: {result['output_dir']}")
    print(f"Metadata: {result['metadata_xlsx']}")
    print(f"Log: {result['log_file']}")
    print(f"Counts: {result['counts']}")
    return 0


def run_geometry_cli(args: argparse.Namespace) -> int:
    if missing_dependency is not None:
        print(dependency_message(), file=sys.stderr)
        return 2
    metadata_path = Path(args.metadata)
    output_dir = Path(args.geometry_output) if args.geometry_output else metadata_path.with_name(GEOMETRY_SCAN_FOLDER)
    geometry = extract_tigre_fdk_geometry(metadata_path, output_dir)
    print(f"Geometry output folder: {output_dir}")
    print(f"Geometry JSON: {output_dir / GEOMETRY_JSON_NAME}")
    print(f"Geometry summary: {output_dir / GEOMETRY_SUMMARY_NAME}")
    print(f"Projection table: {output_dir / GEOMETRY_TABLE_NAME}")
    warnings = geometry.get("warnings", [])
    if warnings:
        print("Warnings:")
        for warning in warnings:
            print(f"- {warning}")
    return 0


def run_self_test() -> int:
    if missing_dependency is not None:
        print(dependency_message(), file=sys.stderr)
        return 2
    assert TXRMReader.dtype_for_code(5) == np.dtype("<u2")
    assert TXRMReader.dtype_for_code(10) == np.dtype("<f4")
    assert sanitize_filename_token("-12.5") == "m12.5"
    assert excel_safe_text("bad\x00cell\x1fname") == "bad cell name"
    assert value_at([1, 2, 3], 1) == 2
    assert value_at([42], 99) == 42
    assert extract_shift_points(
        [{"angle_deg": "1.5", "x_shift_px": "2.25"}, {"angle_deg": "", "x_shift_px": "9"}],
        "x_shift_px",
    ) == ([1.5], [2.25])
    array = np.arange(16, dtype=np.uint16).reshape(4, 4)
    preview = array_to_display_uint8(array + 1000)
    assert preview.dtype == np.uint8
    assert preview.min() == 0
    assert preview.max() == 255
    assert normalize_key_for_matching("Source-to-rotation-axis distance") == "sourcetorotationaxisdistance"
    assert abs(parse_numeric_value(struct.pack("<f", 122.4651489).decode("latin-1") * 2) - 122.4651489) < 1e-5
    assert abs(parse_numeric_value(struct.pack("<f", 3.8473022).decode("latin-1") * 2) - 3.8473022) < 1e-6
    assert parse_numeric_series('{"uint32": 1112014848, "float32": 50.0}') == [50.0]
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        metadata_path = tmp_path / "metadata.xlsx"
        metadata_df = pd.DataFrame(
            [
                {"category": "Geometry", "path": "Geometry/StoRADistance", "parameter": "StoRADistance", "value": -10.0, "unit": "mm"},
                {"category": "Geometry", "path": "Geometry/DtoRADistance", "parameter": "Detector to object distance", "value": r"\x00\x00\xf0A", "unit": "mm"},
                {"category": "Image", "path": "ImageInfo/PixelSize", "parameter": "Pixel size", "value": "2.0 \u00C2\u00B5m", "unit": ""},
                {"category": "Image", "path": "ImageInfo/ImageWidth", "parameter": "Image width", "value": 8, "unit": "px"},
                {"category": "Image", "path": "ImageInfo/ImageHeight", "parameter": "Image height", "value": 8, "unit": "px"},
                {"category": "Image", "path": "Derived/NumPy_dtype", "parameter": "NumPy dtype", "value": "uint16", "unit": ""},
                {"category": "Alignment", "path": "AutoRecon/CenterShift", "parameter": "CenterShift", "value": 0.5, "unit": "px"},
                {"category": "Geometry", "path": "ImageInfo/XrayMagnification", "parameter": "XrayMagnification", "value": 4.0, "unit": ""},
                {"category": "Microscope", "path": "DetAssemblyInfo/LensInfo/LensName", "parameter": "Objective / lens", "value": "20X", "unit": ""},
                {"category": "Acquisition", "path": "ImageInfo/ExposureTime", "parameter": "ExposureTime", "value": 0.5, "unit": "s"},
                {"category": "Microscope", "path": "ImageInfo/XrayVoltage", "parameter": "Voltage", "value": 50.0, "unit": "kV"},
                {"category": "Image", "path": "ImageInfo/CameraBinning", "parameter": "Camera binning", "value": 2, "unit": ""},
                {"category": "Acquisition", "path": "ImageInfo/Temperature", "parameter": "Temperature", "value": 21.5, "unit": "C"},
                {"category": "Geometry", "path": "ImageInfo/ConeAngle", "parameter": "Cone angle", "value": 12.3, "unit": "deg"},
            ]
        )
        projection_df = pd.DataFrame(
            {
                "filename": ["proj_000001.tif", "proj_000002.tif", "proj_000003.tif"],
                "angle_deg": [-180.0, 0.0, 180.0],
                "angle_rad": np.deg2rad([-180.0, 0.0, 180.0]),
                "x_shift_px": [0.0, 1.0, 0.0],
                "y_shift_px": [0.0, 0.0, -1.0],
            }
        )
        images_df = pd.DataFrame(
            [{"role": "projection", "output_name": "proj_000001.tif", "dtype": "uint16", "width_px": 8, "height_px": 8}]
        )
        with pd.ExcelWriter(metadata_path, engine="openpyxl") as writer:
            metadata_df.to_excel(writer, index=False, sheet_name="Metadata")
            projection_df.to_excel(writer, index=False, sheet_name="ProjectionGeometry")
            images_df.to_excel(writer, index=False, sheet_name="ImageStreams")
        geometry = extract_tigre_fdk_geometry(metadata_path, tmp_path / "geometry_output")
        assert geometry["distances"]["DSO_mm"] == 10.0
        assert geometry["distances"]["detector_to_object_distance_mm"] == 30.0
        assert geometry["distances"]["DtoRADistance_mm"] == 30.0
        assert geometry["distances"]["DSD_mm"] == 40.0
        assert geometry["pixel_size"]["dDetector_mm"] == 0.008
        assert geometry["projection_shifts"]["present"] is True
        assert geometry["raw_decoded_metadata"]["Detector to object distance"]["selected_binary_numeric"] == 30.0
        assert geometry["scan_parameters"]["xray_magnification"]["value"] == 4.0
        assert geometry["scan_parameters"]["objective_lens"]["value"] == "20X"
        assert geometry["scan_parameters"]["exposure_time"]["first"] == 0.5
        assert geometry["scan_parameters"]["voltage"]["first"] == 50.0
        assert geometry["scan_parameters"]["camera_binning"]["value"] == 2
        assert geometry["scan_parameters"]["field_of_view"]["x_um"] == 16.0
        assert geometry["scan_parameters"]["temperature"]["first"] == 21.5
        assert geometry["scan_parameters"]["cone_angle"]["value"] == 12.3
        assert (tmp_path / "geometry_output" / GEOMETRY_JSON_NAME).exists()
        assert (tmp_path / "geometry_output" / GEOMETRY_TABLE_NAME).exists()
    print("Self-test passed. Full extraction validation requires a real TXRM/TXM file.")
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=APP_TITLE)
    parser.add_argument("-i", "--input", help="TXRM/TXM file to extract")
    parser.add_argument("-o", "--output", help="Output folder")
    parser.add_argument("--metadata", help="metadata.xlsx file to scan for TIGRE geometry")
    parser.add_argument("--geometry-output", help="Output folder for TIGRE geometry scan files")
    parser.add_argument("--self-test", action="store_true", help="Run lightweight internal checks")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    if args.self_test:
        return run_self_test()
    if args.metadata:
        return run_geometry_cli(args)
    if args.input:
        return run_cli(args)
    app = TXRMApp()
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
