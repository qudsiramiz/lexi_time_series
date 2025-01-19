import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from pathlib import Path
import re
import glob
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import os


def list_files_recursively(service, folder_id):
    """Recursively list all files (including in subfolders) within a given folder."""
    all_files = []

    # Query to get all items in the current folder
    query = f"'{folder_id}' in parents and trashed=false"
    results = (
        service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    )
    files = results.get("files", [])

    for file in files:
        # If the file is a folder, recurse into it
        if file["mimeType"] == "application/vnd.google-apps.folder":
            all_files.extend(list_files_recursively(service, file["id"]))
        else:
            # Add files that are not folders
            all_files.append(file)

    return all_files


def prepare_data_from_drive():
    # Path to the service account key file
    try:
        service_account_file = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
    except Exception:
        service_account_file = "lexi-time-series-2f0841418a28.json"
        print(
            "Service account file not found in environment variables. Using local file."
        )

    # Define the scope for accessing Google Drive
    scopes = ["https://www.googleapis.com/auth/drive.readonly"]

    # Authenticate with the service account credentials
    credentials = Credentials.from_service_account_file(
        service_account_file, scopes=scopes
    )
    drive_service = build("drive", "v3", credentials=credentials)

    # Folder ID from the shared Google Drive folder link
    folder_id = "1LMd-rEBSgmzZ6Y9Ggzq7In9O1bk6LRYa"

    # List files in the folder
    # query = f"'{folder_id}' in parents and mimeType='text/csv'"
    # results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = list_files_recursively(drive_service, folder_id)

    # Only keep CSV files
    files = [file for file in files if file["name"].endswith("hk_output.csv")]

    # Exclude pattern
    exclude_pattern = re.compile(r"payload_lexi_\d+_\d+_\d+_\d+_hk_output.csv")
    files = [file for file in files if not exclude_pattern.search(file["name"])]

    print(f"Found {len(files)} CSV files in the folder.")
    # return files

    # Load all CSV files into a single DataFrame
    dataframes = []
    for i, file in enumerate(files):
        print(f"Downloading file {i + 1} of {len(files)}: {file['name']}")
        file_id = file["id"]
        request = drive_service.files().get_media(fileId=file_id)
        file_data = io.BytesIO()
        downloader = MediaIoBaseDownload(file_data, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        file_data.seek(0)
        df = pd.read_csv(file_data)
        dataframes.append(df)

    # Combine all DataFrames into one
    df = pd.concat(dataframes, ignore_index=True)

    # Set the "Date" column as the index
    df["Date"] = pd.to_datetime(df["Date"])
    df.set_index("Date", inplace=True)

    return df


# Prepare the data
def prepare_data():
    # Load all CSV files in the directory
    parent_folder = Path("orbit/")
    file_name_format = "payload_lexi_*_*_hk_output.csv"
    csv_files = glob.glob(str(parent_folder / "**" / file_name_format), recursive=True)
    exclude_pattern = re.compile(r"payload_lexi_\d+_\d+_\d+_\d+_hk_output.csv")
    csv_files = [file for file in csv_files if not exclude_pattern.search(file)]

    # Load all CSV files into a single DataFrame
    df = pd.concat([pd.read_csv(file) for file in csv_files], ignore_index=True)

    # Set the "Date" column as the index
    df["Date"] = pd.to_datetime(df["Date"])
    df.set_index("Date", inplace=True)

    return df


# Step 2: Initialize Dash App
app = dash.Dash(__name__)

# Load the data
# df = prepare_data_from_drive()
df = prepare_data()

# Step 3: Layout of the Web Application
app.layout = html.Div(
    [
        html.H1("Interactive Time Series Plot"),
        # Dropdown for selecting a parameter (key)
        html.Label("Select Parameter:"),
        dcc.Dropdown(
            id="parameter-dropdown",
            options=[
                {"label": col, "value": col} for col in df.columns if col != "Date"
            ],
            value="+28V_Imon",  # Default value
        ),
        # Date Picker Range for selecting time range
        html.Label("Select Time Range:"),
        dcc.DatePickerRange(
            id="date-picker-range",
            start_date=df.index.min().date(),
            end_date=df.index.max().date() + pd.Timedelta(days=1),
            display_format="YYYY-MM-DD",
            style={"margin": "10px"},
        ),
        # Graph to show the time series
        dcc.Graph(id="time-series-plot"),
    ]
)


# Step 4: Create Callback to Update Plot based on User Inputs
@app.callback(
    Output("time-series-plot", "figure"),
    [
        Input("parameter-dropdown", "value"),
        Input("date-picker-range", "start_date"),
        Input("date-picker-range", "end_date"),
    ],
)
def update_plot(selected_param, start_date, end_date):
    # Filter data by selected time range
    mask = (df.index >= start_date) & (df.index <= end_date)
    filtered_df = df[mask]

    # Create the plotly figure
    trace = go.Scatter(
        x=filtered_df.index,
        y=filtered_df[selected_param],
        mode="lines",
        name=selected_param,
    )

    # For each day, calculate the median, 10th percentile, and 90th percentile
    daily_stats = (
        filtered_df[selected_param]
        .resample("D")
        .agg(["median", lambda x: x.quantile(0.1), lambda x: x.quantile(0.9)])
    )

    # Rename the columns for clarity
    daily_stats.columns = ["median", "10th_percentile", "90th_percentile"]

    # Add the date as a column for plotting
    daily_stats["date"] = daily_stats.index
    daily_stats["date_str"] = daily_stats.index.strftime("%Y-%m-%d")

    daily_stats.reset_index(drop=True, inplace=True)
    trace_avg = go.Scatter(
        x=daily_stats["date"] + pd.Timedelta(hours=12),
        y=daily_stats["median"],
        mode="markers",
        marker=dict(size=10, color="rgba(0, 0, 0, 1)", symbol="diamond"),
        name="Daily Average",
    )

    trace_error = go.Scatter(
        x=daily_stats["date"] + pd.Timedelta(hours=12),
        y=daily_stats["median"],
        error_y=dict(
            type="data",
            symmetric=False,
            array=daily_stats["median"] - daily_stats["10th_percentile"],
            arrayminus=daily_stats["90th_percentile"] - daily_stats["median"],
        ),
        mode="markers",
        marker=dict(size=10, color="rgba(255, 0, 0, 0.5)", symbol="circle"),
        name="10th-90th Percentile",
    )

    layout = go.Layout(
        title=f"Time Series of {selected_param}",
        xaxis={"title": "Time [UTC]"},
        yaxis={"title": selected_param},
        hovermode="closest",
    )

    return {
        "data": [trace, trace_avg, trace_error],
        "layout": layout,
    }
