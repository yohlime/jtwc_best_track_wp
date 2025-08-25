import pandas as pd

from const import COL_NAMES
from helper.utils import format_types


def coord_str_to_num(coord_str: str):
    """Convert a string coordinate to float

    Params:
    -------
    coord_str: str
    The value to be converted

    Returns:
    --------
    coord: float
    The converted value
    """
    coord_dir = coord_str[-1]
    coord = float(coord_str[:-1]) / 10
    if (coord_dir == "S") | (coord_dir == "W"):
        coord = -coord
    return coord


def parse_input(filepath_or_buffer):
    """Parse the input into a DataFrame

    Params:
    -------
    filepath_or_buffer: str, StringIO
    The input

    Returns:
    --------
    out_df: pd.DataFrame
    The output dataframe
    """
    max_cols = 50
    ncols = len(COL_NAMES) - 1
    _df = pd.read_csv(
        filepath_or_buffer,
        header=None,
        names=range(max_cols),
        skipinitialspace=True,
    )
    _df = _df.loc[:, :ncols]
    _df.columns = COL_NAMES
    _df = _df.loc[_df["BASIN"] == "WP"]
    _df.drop(_df.columns[[0, 3, 4, 5]], axis=1, inplace=True)
    _df["YYYYMMDDHH"] = pd.to_datetime(_df["YYYYMMDDHH"], format="%Y%m%d%H")

    _df2 = pd.DataFrame(_df["CY"])

    _df2["YYYY"] = _df["YYYYMMDDHH"].dt.year
    _df2["MM"] = _df["YYYYMMDDHH"].dt.month
    _df2["DD"] = _df["YYYYMMDDHH"].dt.day
    _df2["HH"] = _df["YYYYMMDDHH"].dt.hour
    _df2["LAT"] = _df["LAT"].apply(coord_str_to_num)
    _df2["LON"] = _df["LON"].apply(coord_str_to_num)

    _out_df = pd.concat([_df2, _df.iloc[:, 4:]], axis=1)

    return format_types(_out_df)
