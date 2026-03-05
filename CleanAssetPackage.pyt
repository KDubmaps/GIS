# -*- coding: utf-8 -*-
"""
UN Asset Package Cleanup Toolbox (.pyt)

ArcGIS Python toolbox to find and optionally delete duplicate tables and
feature classes in a Utility Network asset package (file geodatabase).

Features:
- Scans tables and feature classes (including those inside the UtilityNetwork dataset)
- Detects likely duplicates by stripping common suffixes (_1, _2, _copy, _backup, _old)
- Chooses a single keeper per base name (prefers unsuffixed, then highest row count, then field count)
- Generates a CSV report of intended actions
- (Optional) Creates a full backup of the .gdb and deletes non-keepers

Usage:
    Add toolbox to ArcGIS Pro > Geoprocessing pane > Run "Clean Asset Package Duplicates"

Author: M365 Copilot
"""

import arcpy
import os
import re
import shutil
import csv
from datetime import datetime

# ----------------------
# Helper functions
# ----------------------

def _normalize_base_name(name: str, regex: re.Pattern) -> str:
    return regex.sub("", name)


def _is_gdb_system(name: str) -> bool:
    return name.upper().startswith("GDB_")


def _list_all_objects(workspace, un_dataset_name="UtilityNetwork"):
    arcpy.env.workspace = workspace
    result = {
        "tables": list(arcpy.ListTables() or []),
        "feature_classes": list(arcpy.ListFeatureClasses() or [])
    }
    # Include FCs inside the UtilityNetwork dataset if present
    for fds in arcpy.ListDatasets(feature_type='feature') or []:
        if os.path.basename(fds).lower() == (un_dataset_name or "").lower():
            for fc in arcpy.ListFeatureClasses(feature_dataset=fds) or []:
                result["feature_classes"].append(os.path.join(fds, fc))
    return result


def _name_only(path):
    return os.path.basename(path)


def _safe_get_count(path):
    try:
        return int(arcpy.management.GetCount(path)[0])
    except Exception:
        return -1


def _field_count(path):
    try:
        return len(arcpy.ListFields(path) or [])
    except Exception:
        return -1


def _choose_keeper(candidates):
    """Choose keeper among candidates for the same base name.
    candidates items contain: path, name, base_name, rows, fields
    Priority: unsuffixed == base_name, else max rows, else max fields, else shortest name
    """
    # Prefer unsuffixed (name equals base_name, case-insensitive)
    unsuffixed = [c for c in candidates if c["name"].lower() == c["base_name"].lower()]
    if unsuffixed:
        return unsuffixed[0]

    # Highest row count
    max_rows = max(c["rows"] for c in candidates)
    top = [c for c in candidates if c["rows"] == max_rows]

    # Highest field count
    if len(top) > 1:
        max_fields = max(c["fields"] for c in top)
        top = [c for c in top if c["fields"] == max_fields]

    # Shortest name (least edited)
    if len(top) > 1:
        top.sort(key=lambda c: len(c["name"]))
    return top[0]


def _backup_gdb(gdb_path):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    parent = os.path.dirname(gdb_path.rstrip(os.sep))
    base = os.path.basename(gdb_path.rstrip(os.sep))
    backup_name = f"{base}_backup_{ts}"
    backup_path = os.path.join(parent, backup_name)
    shutil.copytree(gdb_path, backup_path)
    return backup_path


# ----------------------
# Python Toolbox classes
# ----------------------

class Toolbox(object):
    def __init__(self):
        self.label = "UN Asset Package Cleanup"
        self.alias = "UNAPCleanup"
        self.tools = [CleanAssetPackage]


class CleanAssetPackage(object):
    def __init__(self):
        self.label = "Clean Asset Package Duplicates"
        self.description = (
            "Find and optionally delete duplicate tables and feature classes in a Utility Network "
            "asset package (.gdb). Creates a CSV report and, if deletion is enabled, a backup of the FGDB."
        )
        self.canRunInBackground = True

    def getParameterInfo(self):
        p0 = arcpy.Parameter(
            displayName="Asset Package Geodatabase",
            name="asset_package_gdb",
            datatype="Workspace",
            parameterType="Required",
            direction="Input"
        )
        p0.filter.list = ["File Geodatabase"]

        p1 = arcpy.Parameter(
            displayName="Delete Duplicates (otherwise report only)",
            name="delete_duplicates",
            datatype="Boolean",
            parameterType="Optional",
            direction="Input"
        )
        p1.value = False

        p2 = arcpy.Parameter(
            displayName="Suffix Patterns (regex, '|' separated)",
            name="suffix_patterns",
            datatype="String",
            parameterType="Optional",
            direction="Input"
        )
        p2.value = r"_(\d+)$|_copy(\d+)?$|_backup(\d+)?$|_old(\d+)?$"

        p3 = arcpy.Parameter(
            displayName="Utility Network Dataset Name",
            name="un_dataset_name",
            datatype="String",
            parameterType="Optional",
            direction="Input"
        )
        p3.value = "UtilityNetwork"

        p4 = arcpy.Parameter(
            displayName="CSV Report Output Folder (optional)",
            name="report_folder",
            datatype="Folder",
            parameterType="Optional",
            direction="Input"
        )

        p5 = arcpy.Parameter(
            displayName="Report CSV (derived)",
            name="report_csv",
            datatype="DEFile",
            parameterType="Derived",
            direction="Output"
        )

        return [p0, p1, p2, p3, p4, p5]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        ap_gdb = parameters[0].valueAsText
        if ap_gdb and not ap_gdb.lower().endswith('.gdb'):
            parameters[0].setErrorMessage("Input must be a File Geodatabase (.gdb) asset package.")
        return

    def execute(self, parameters, messages):
        ap_gdb = parameters[0].valueAsText
        do_delete = bool(parameters[1].value)
        suffix_patterns = parameters[2].valueAsText or r"_(\d+)$|_copy(\d+)?$|_backup(\d+)?$|_old(\d+)?$"
        un_dataset_name = parameters[3].valueAsText or "UtilityNetwork"
        report_folder = parameters[4].valueAsText

        if not os.path.isdir(ap_gdb):
            raise arcpy.ExecuteError("Asset Package path is not a valid folder.")

        # Compile suffix regex
        try:
            suffix_regex = re.compile("(" + suffix_patterns + ")", re.IGNORECASE)
        except re.error as ex:
            raise arcpy.ExecuteError(f"Invalid regex in 'Suffix Patterns': {ex}")

        # Determine report path
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        if report_folder and os.path.isdir(report_folder):
            csv_path = os.path.join(report_folder, f"asset_package_cleanup_report_{ts}.csv")
        else:
            # place inside the FGDB folder (FGDB is a directory on disk)
            csv_path = os.path.join(ap_gdb, f"asset_package_cleanup_report_{ts}.csv")
        parameters[5].value = csv_path

        arcpy.AddMessage("Scanning asset package for tables and feature classes…")
        objects = _list_all_objects(ap_gdb, un_dataset_name=un_dataset_name)
        tables = objects.get("tables", [])
        fcs = objects.get("feature_classes", [])

        arcpy.AddMessage(f"Found {len(tables)} tables and {len(fcs)} feature classes (including dataset FCs).")

        dup_groups = {}
        report_rows = []

        def consider(path, kind):
            name = _name_only(path)
            if _is_gdb_system(name):
                return
            base_name = _normalize_base_name(name, suffix_regex)
            key = (kind, base_name.lower())
            dup_groups.setdefault(key, []).append((path, name, base_name))

        for t in tables:
            consider(t, "table")
        for fc in fcs:
            consider(fc, "feature_class")

        to_delete = []
        total_groups = 0
        duplicate_groups = 0

        for (kind, base_lower), items in sorted(dup_groups.items(), key=lambda x: (x[0][0], x[0][1])):
            total_groups += 1
            if len(items) == 1:
                path, name, base_name = items[0]
                report_rows.append({
                    "kind": kind, "name": name, "base_name": base_name, "action": "keep",
                    "reason": "single_instance", "rows": _safe_get_count(path),
                    "fields": _field_count(path), "path": path
                })
                continue

            duplicate_groups += 1
            candidates = []
            for path, name, base_name in items:
                candidates.append({
                    "path": path,
                    "name": name,
                    "base_name": base_name,
                    "rows": _safe_get_count(path),
                    "fields": _field_count(path)
                })
            keeper = _choose_keeper(candidates)

            for c in candidates:
                action = "keep" if c["path"] == keeper["path"] else "delete"
                report_rows.append({
                    "kind": kind,
                    "name": c["name"],
                    "base_name": c["base_name"],
                    "action": action,
                    "reason": "duplicate_suffix",
                    "rows": c["rows"],
                    "fields": c["fields"],
                    "path": c["path"]
                })
                if action == "delete":
                    to_delete.append(c["path"])

        # Write CSV report
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["kind","name","base_name","action","reason","rows","fields","path"])
            w.writeheader()
            for r in report_rows:
                w.writerow(r)

        arcpy.AddMessage(f"Report written: {csv_path}")
        arcpy.AddMessage(f"Groups analyzed: {total_groups}; groups with duplicates: {duplicate_groups}")

        if do_delete and to_delete:
            arcpy.AddWarning("Deletion enabled. Creating full backup of the FGDB before deleting…")
            backup_path = _backup_gdb(ap_gdb)
            arcpy.AddMessage(f"Backup created: {backup_path}")

            # Set workspace so dataset-relative paths are valid
            arcpy.env.workspace = ap_gdb
            failures = 0
            for path in to_delete:
                try:
                    arcpy.AddMessage(f"Deleting: {path}")
                    arcpy.management.Delete(path)
                except Exception as ex:
                    failures += 1
                    arcpy.AddWarning(f"Could not delete {path}: {ex}")
            if failures:
                arcpy.AddWarning(f"Completed with {failures} deletion failure(s). See messages and report.")
            else:
                arcpy.AddMessage("All non-keeper duplicates deleted successfully.")
        elif do_delete and not to_delete:
            arcpy.AddMessage("Deletion enabled, but no duplicates were found. Nothing to delete.")
        else:
            arcpy.AddMessage("Report-only run complete. Review the CSV. Re-run with 'Delete Duplicates' = True if satisfied.")
