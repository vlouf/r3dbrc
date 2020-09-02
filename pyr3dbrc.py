#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script for finding the consecutive CPOL and Berrimah files at a given date.

@title: pyr3dbrc.py
@author: Valentin Louf <valentin.louf@bom.gov.au>
@institutions: Australian Bureau of Meteorology
@date: 02/09/2020

.. autosummary::
    :toctree: generated/

    get_radar_archive_file
    extract_zip
    get_cpol_file
    main
"""
import os
import re
import glob
import argparse
import datetime
import zipfile

import numpy as np
import pandas as pd


def get_radar_archive_file(date, rid: str) -> str:
    """
    Return the archive containing the radar file for a given radar ID and a
    given date.

    Parameters:
    ===========
    date: datetime
        Date.
    rid: str
        Radar RAPIC ID.

    Returns:
    ========
    file: str
        Radar archive if it exists at the given date.
    """
    if type(rid) is not str:
        rid = f"{rid:02}"

    datestr = date.strftime("%Y%m%d")
    file = f"/g/data/rq0/admin/level_1b/grid_150km/{rid}/{date.year}/{rid}_{datestr}_level1b_grid_150km.zip"
    if not os.path.exists(file):
        raise FileNotFoundError(f"{file} does not exist for radar {rid}.")

    return file


def extract_zip(inzip: str, date) -> str:
    """
    Extract file in a daily archive zipfile for a specific datetime.

    Parameters:
    ===========
    inzip: str
        Input zipfile
    date: pd.Timestamp
        Which datetime we want to extract.

    Returns:
    ========
    grfile: str
        Output ground radar file.
    """
    def get_zipfile_name(namelist, date):
        datestr = [re.findall("[0-9]{8}_[0-9]{6}", n)[0] for n in namelist]
        timestamps = np.array([datetime.datetime.strptime(dt, "%Y%m%d_%H%M%S") for dt in datestr], dtype="datetime64")
        pos = np.argmin(np.abs(timestamps - date.to_numpy()))
        grfile = namelist[pos]
        return grfile

    with zipfile.ZipFile(inzip) as zid:
        namelist = zid.namelist()
        file = get_zipfile_name(namelist, date)
        zid.extract(file, path=UNZIPPATH)

    grfile = os.path.join(UNZIPPATH, file)

    return grfile


def get_cpol_file(date) -> str:
    """
    Find the CPOL gridded data for given date.

    Parameters:
    ===========
    date: datetime
        Date.

    Returns:
    ========
    file: str
        CPOL file for the given date if it exists.
    """
    datestr = date.strftime("%Y%m%d")
    path = f"/scratch/kl02/vhl548/cpol_level_1b/v2020/gridded/grid_150km_1000m/{date.year}/{datestr}/*.nc"
    namelist = sorted(glob.glob(path))
    if len(namelist) == 0:
        raise FileNotFoundError(f"No CPOL file found for this date {datestr}.")

    datelist = [re.findall("[0-9]{8}.[0-9]{6}", n)[0] for n in namelist]
    timestamps = np.array([datetime.datetime.strptime(dt, "%Y%m%d.%H%M%S") for dt in datelist], dtype="datetime64")
    pos = np.argmin(np.abs(timestamps - date.to_numpy()))
    grfile = namelist[pos]

    return grfile


def main():
    date = [INDATE]
    date.append(date[0] + pd.Timedelta("10Min"))
    files = [get_cpol_file(d) for d in date]
    zips = [get_radar_archive_file(d, 64) for d in date]

    for z, d in zip(zips, date):
        files.append(extract_zip(z, d))

    with open(os.path.join(OUTPATH, "r3dbrc"), "w+") as fid:
        fid.write("\n".join(files))

    return None


if __name__ == "__main__":
    parser_description = "Generate the r3dbrc file for 3D Winds."
    parser = argparse.ArgumentParser(description=parser_description)
    parser.add_argument("-r", "--rid", dest="rid", type=int, help="Radar Rapic ID", default=64)
    parser.add_argument(
        "-o", "--output", dest="outdir", type=str, help="Output directory.", default=os.curdir,
    )
    parser.add_argument(
        "-u",
        "--unzip",
        dest="unzip",
        type=str,
        help="Unzipping temporary directory (files won't be deleted!).",
        default=os.curdir,
    )
    parser.add_argument(
        "-d", "--date", dest="date", type=str, help="Datetime format: 201703041210 or 2017-03-04T12:10", required=True
    )

    args = parser.parse_args()
    try:
        INDATE = pd.Timestamp(args.date)
    except Exception:
        raise ValueError("Input date not understood.")
    OUTPATH = args.outdir
    UNZIPPATH = args.unzip
    RID = args.rid

    main()
