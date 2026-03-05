# -*- coding: utf-8 -*-
"""
Enterprise GDB Clean Slate Toolbox (.pyt) — FIX 3

Purpose
-------
Erase user-created datasets (feature classes, tables, feature datasets, relationship classes, and DB views)
from an **Enterprise Geodatabase** while preserving geodatabase system tables. Includes dry-run report and
an option to block/disconnect users (admin only).

Notes
-----
- Parameters are defined using **keyword arguments only** for maximum compatibility.
- Relationship classes and datasets are discovered with arcpy.da.Walk.
- Database views are discovered via arcpy.ListTables() + Describe.isView.
- Deletions use arcpy.management.Delete to allow ArcGIS to clean up geodatabase metadata.

Author: M365 Copilot
"""

import arcpy
import os
import csv
from datetime import datetime

# ----------------------
# Helpers
# ----------------------

def _is_system_name(name: str) -> bool:
    if not name:
        return False
    u = name.upper()
    return u.startswith('SDE_') or u.startswith('GDB_')


def _owner_ok(desc_obj, allowed_owners):
    try:
        owner = getattr(desc_obj, 'owner', None)
    except Exception:
        owner = None
    if allowed_owners:
        return (owner or '').lower() in allowed_owners
    return True


def _row(dt, path, name, owner, action, reason, note=""):
    return {
        'type': dt,
        'path': path,
        'name': name,
        'owner': owner or '',
        'action': action,
        'reason': reason,
        'note': note
    }


def _write_csv(out_csv, rows):
    with open(out_csv, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['type','path','name','owner','action','reason','note'])
        w.writeheader()
        for r in rows:
            w.writerow(r)


# ----------------------
# Toolbox and Tool
# ----------------------

class Toolbox(object):
    def __init__(self):
        self.label = "Enterprise GDB Clean Slate"
        self.alias = "egdb_cleanslate"
        self.tools = [EraseUserData]


class EraseUserData(object):
    def __init__(self):
        self.label = "Erase User Data (Enterprise Geodatabase)"
        self.description = (
            "Removes user-created datasets (feature classes, tables, feature datasets, relationship classes, DB views) "
            "from an Enterprise Geodatabase while preserving system tables. Supports dry-run preview and optional "
            "disconnect of users."
        )
        self.canRunInBackground = True

    def getParameterInfo(self):
        p0 = arcpy.Parameter(
            displayName="Enterprise Geodatabase Connection (.sde)",
            name="sde_connection",
            datatype="Workspace",
            parameterType="Required",
            direction="Input"
        )
        # Do not set filter.list to avoid compatibility issues across Pro versions

        p1 = arcpy.Parameter(
            displayName="Dry Run (report only)",
            name="dry_run",
            datatype="Boolean",
            parameterType="Optional",
            direction="Input"
        )
        p1.value = True

        p2 = arcpy.Parameter(
            displayName="Block New Connections & Disconnect Users (requires admin)",
            name="disconnect_users",
            datatype="Boolean",
            parameterType="Optional",
            direction="Input"
        )
        p2.value = False

        p3 = arcpy.Parameter(
            displayName="Restrict to Owners (comma-separated, optional)",
            name="owners",
            datatype="String",
            parameterType="Optional",
            direction="Input"
        )

        p4 = arcpy.Parameter(
            displayName="Output Report Folder (optional)",
            name="out_folder",
            datatype="Folder",
            parameterType="Optional",
            direction="Input"
        )

        p5 = arcpy.Parameter(
            displayName="Deletion Report (derived)",
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
        sde_conn = parameters[0].valueAsText
        if sde_conn and not sde_conn.lower().endswith('.sde'):
            parameters[0].setWarningMessage("Input should be an .sde connection file to an enterprise geodatabase.")
        return

    def execute(self, parameters, messages):
        sde_conn = parameters[0].valueAsText
        dry_run = bool(parameters[1].value)
        do_disconnect = bool(parameters[2].value)
        owners_raw = parameters[3].valueAsText
        out_folder = parameters[4].valueAsText

        allowed_owners = None
        if owners_raw:
            allowed_owners = {o.strip().lower() for o in owners_raw.split(',') if o.strip()}

        arcpy.AddMessage(f"Connecting to: {sde_conn}")
        arcpy.env.workspace = sde_conn
        arcpy.env.overwriteOutput = True
        arcpy.ClearWorkspaceCache_management()

        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_name = f"egdb_clean_slate_{ts}.csv"
        report_csv = os.path.join(out_folder if (out_folder and os.path.isdir(out_folder)) else os.path.dirname(sde_conn), base_name)
        parameters[5].value = report_csv

        rows = []

        # Optionally block and disconnect users (admin only)
        if do_disconnect:
            try:
                arcpy.AddWarning("Blocking new connections and disconnecting users…")
                arcpy.AcceptConnections(sde_conn, False)
                arcpy.DisconnectUser(sde_conn, "ALL")
                rows.append(_row('admin', sde_conn, os.path.basename(sde_conn), '', 'admin', 'blocked+disconnected'))
            except Exception as ex:
                arcpy.AddWarning(f"Could not block/disconnect users: {ex}")
                rows.append(_row('admin', sde_conn, os.path.basename(sde_conn), '', 'admin', 'disconnect_failed', str(ex)))

        # Build plan in dependency-aware order
        # 1) Relationship classes (via Walk)
        try:
            for dirpath, dirnames, filenames in arcpy.da.Walk(sde_conn, datatype=['RelationshipClass']):
                for nm in filenames:
                    path = os.path.join(dirpath, nm)
                    try:
                        d = arcpy.Describe(path)
                        if not _owner_ok(d, allowed_owners):
                            continue
                        rows.append(_row('RelationshipClass', path, d.name, getattr(d, 'owner', None), 'delete' if not dry_run else 'preview', 'relationship_class'))
                    except Exception as ex:
                        rows.append(_row('RelationshipClass', path, nm, '', 'skip', 'describe_failed', str(ex)))
        except Exception as ex:
            arcpy.AddWarning(f"Walk(RelationshipClass) failed: {ex}")

        # 2) Controller datasets (ParcelFabric, UtilityNetwork, Topology, NetworkDataset)
        def add_controller(list_type, label):
            try:
                items = arcpy.ListDatasets("*", list_type) or []
            except Exception:
                items = []
            for path in items:
                try:
                    d = arcpy.Describe(path)
                    if not _owner_ok(d, allowed_owners):
                        continue
                    rows.append(_row(label, path, d.name, getattr(d, 'owner', None), 'delete' if not dry_run else 'preview', 'controller_dataset'))
                except Exception as ex:
                    rows.append(_row(label, path, os.path.basename(path), '', 'skip', 'describe_failed', str(ex)))

        add_controller("ParcelFabric", "ParcelFabric")
        add_controller("UtilityNetwork", "UtilityNetwork")
        add_controller("Topology", "Topology")
        # Some Pro versions use "Network" or "Network Dataset"; try both
        add_controller("Network", "NetworkDataset")
        add_controller("Network Dataset", "NetworkDataset")

        # 3) Feature classes & tables (via Walk)
        try:
            for dirpath, dirnames, filenames in arcpy.da.Walk(sde_conn, datatype=["FeatureClass", "Table"]):
                for nm in filenames:
                    path = os.path.join(dirpath, nm)
                    try:
                        d = arcpy.Describe(path)
                        owner = getattr(d, 'owner', None)
                        if not _owner_ok(d, allowed_owners):
                            continue
                        if _is_system_name(d.name):
                            rows.append(_row(d.dataType, path, d.name, owner, 'skip', 'system_like_name'))
                            continue
                        rows.append(_row(d.dataType, path, d.name, owner, 'delete' if not dry_run else 'preview', 'user_dataset'))
                    except Exception as ex:
                        rows.append(_row('Unknown', path, nm, '', 'skip', 'describe_failed', str(ex)))
        except Exception as ex:
            arcpy.AddWarning(f"Walk(FeatureClass/Table) failed: {ex}")

        # 3b) Database views via ListTables + Describe.isView
        try:
            for t in arcpy.ListTables() or []:
                try:
                    d = arcpy.Describe(t)
                    if getattr(d, 'isView', False):
                        if not _owner_ok(d, allowed_owners):
                            continue
                        rows.append(_row('View', t, d.name, getattr(d, 'owner', None), 'delete' if not dry_run else 'preview', 'db_view'))
                except Exception:
                    continue
        except Exception:
            pass

        # 4) Feature datasets last
        try:
            fds_list = list(arcpy.ListDatasets(feature_type='feature') or [])
            fds_list.sort(key=lambda p: (p.count(os.sep), len(p)), reverse=True)
            for fds in fds_list:
                try:
                    d = arcpy.Describe(fds)
                    if not _owner_ok(d, allowed_owners):
                        continue
                    rows.append(_row('FeatureDataset', fds, d.name, getattr(d, 'owner', None), 'delete' if not dry_run else 'preview', 'feature_dataset'))
                except Exception as ex:
                    rows.append(_row('FeatureDataset', fds, os.path.basename(fds), '', 'skip', 'describe_failed', str(ex)))
        except Exception as ex:
            arcpy.AddWarning(f"ListDatasets(feature) failed: {ex}")

        # Write plan
        _write_csv(report_csv, rows)
        arcpy.AddMessage(f"Plan written: {report_csv}")

        if dry_run:
            arcpy.AddMessage("Dry run complete. No data were deleted.")
            return

        # Execute deletions
        failures = 0
        for r in rows:
            if r['action'] != 'delete':
                continue
            try:
                arcpy.AddMessage(f"Deleting {r['type']}: {r['path']}")
                arcpy.management.Delete(r['path'])
                r['note'] = (r.get('note','') + ' [DELETED]').strip()
            except Exception as ex:
                failures += 1
                r['note'] = (r.get('note','') + f' [FAILED: {ex}]').strip()
                arcpy.AddWarning(f"Failed to delete {r['path']}: {ex}")

        _write_csv(report_csv, rows)
        if failures:
            arcpy.AddWarning(f"Completed with {failures} failure(s). See report for details: {report_csv}")
        else:
            arcpy.AddMessage("All targeted user datasets deleted successfully.")

        # Re-enable connections if previously disabled
        if do_disconnect:
            try:
                arcpy.AcceptConnections(sde_conn, True)
                arcpy.AddMessage("Re-enabled new connections to the geodatabase.")
            except Exception as ex:
                arcpy.AddWarning(f"Could not re-enable connections: {ex}")
