#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import re
import glob
import argparse
import datetime
import zipfile

import numpy as np
import pandas as pd


def get_radar_archive_file(date, rid):
    """
    Return the archive containing the radar file for a given radar ID and a
    given date.

    Parameters:
    ===========
    date: datetime
        Date.
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
        print(f"{file} does not exist.")
        return None

    return file


def extract_zip(inzip, date, path="/scratch/kl02/vhl548/unzipdir"):
    """
    Extract file in a daily archive zipfile for a specific datetime.

    Parameters:
    ===========
    inzip: str
        Input zipfile
    date: pd.Timestamp
        Which datetime we want to extract.
    path: str
        Path where we want to temporarly store the output file.
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
        zid.extract(file, path=path)

    grfile = os.path.join(path, file)

    return grfile


def get_cpol_file(date):
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
    input_date = datetime.datetime(2014, 12, 1, 6, 30)
    date = [pd.Timestamp(input_date)]
    date.append(date[0] + pd.Timedelta("10Min"))
    files = [get_cpol_file(d) for d in date]
    zips = [get_radar_archive_file(d, 64) for d in date]

    for z, d in zip(zips, date):
        files.append(extract_zip(z, d))

    with open("r3dbrc", "w+") as fid:
        fid.write("\n".join(files))

    return None

if __name__ == "__main__":
    main()