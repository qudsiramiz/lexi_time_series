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
import json


# Utility function to list files recursively from Google Drive
def list_files_recursively(service, folder_id):
    all_files = []
    query = f"'{folder_id}' in parents and trashed=false"
    results = (
        service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    )
    files = results.get("files", [])
    for file in files:
        if file["mimeType"] == "application/vnd.google-apps.folder":
            all_files.extend(list_files_recursively(service, file["id"]))
        else:
            all_files.append(file)
    return all_files


# Function to prepare data from Google Drive
def prepare_data_from_drive():
    try:
        # Load service account credentials from environment variable
        service_account_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_KEY"])
        scopes = ["https://www.googleapis.com/auth/drive.readonly"]
        credentials = Credentials.from_service_account_info(
            service_account_info, scopes=scopes
        )
        drive_service = build("drive", "v3", credentials=credentials)

        folder_id = "1LMd-rEBSgmzZ6Y9Ggzq7In9O1bk6LRYa"
        files = list_files_recursively(drive_service, folder_id)

        files = [file for file in files if file["name"].endswith("hk_output.csv")]
        exclude_pattern = re.compile(r"payload_lexi_\d+_\d+_\d+_\d+_hk_output.csv")
        files = [file for file in files if not exclude_pattern.search(file["name"])]

        print(f"Found {len(files)} CSV files in the folder.")
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

        df = pd.concat(dataframes, ignore_index=True)
        df["Date"] = pd.to_datetime(df["Date"])
        df.set_index("Date", inplace=True)
        return df

    except Exception as e:
        print(f"Error preparing data from Google Drive: {e}")
        return pd.DataFrame()


# Function to prepare data from local orbit folder
def prepare_data():
    try:
        parent_folder = Path("orbit/")
        file_name_format = "payload_lexi_*_*_hk_output.csv"
        csv_files = glob.glob(
            str(parent_folder / "**" / file_name_format), recursive=True
        )
        exclude_pattern = re.compile(r"payload_lexi_\d+_\d+_\d+_\d+_hk_output.csv")
        csv_files = [file for file in csv_files if not exclude_pattern.search(file)]

        if not csv_files:
            print("No CSV files found in the orbit folder.")
            return pd.DataFrame()

        df = pd.concat([pd.read_csv(file) for file in csv_files], ignore_index=True)
        df["Date"] = pd.to_datetime(df["Date"])
        df.set_index("Date", inplace=True)
        return df

    except Exception as e:
        print(f"Error preparing data from local files: {e}")
        return pd.DataFrame()


# Initialize Dash App
app = dash.Dash(__name__)

# Load the data
df = prepare_data_from_drive() or prepare_data()
if df.empty:
    print("Warning: No data available to display.")

# Layout of the Web Application
app.layout = html.Div(
    [
        html.H1("Interactive Time Series Plot"),
        html.Label("Select Parameter:"),
        dcc.Dropdown(
            id="parameter-dropdown",
            options=[
                {"label": col, "value": col} for col in df.columns if col != "Date"
            ],
            value=df.columns[0] if not df.empty else None,
        ),
        html.Label("Select Time Range:"),
        dcc.DatePickerRange(
            id="date-picker-range",
            start_date=df.index.min().date() if not df.empty else None,
            end_date=(
                df.index.max().date() + pd.Timedelta(days=1) if not df.empty else None
            ),
            display_format="YYYY-MM-DD",
            style={"margin": "10px"},
        ),
        dcc.Graph(id="time-series-plot"),
    ]
)


# Callback to Update Plot
@app.callback(
    Output("time-series-plot", "figure"),
    [
        Input("parameter-dropdown", "value"),
        Input("date-picker-range", "start_date"),
        Input("date-picker-range", "end_date"),
    ],
)
def update_plot(selected_param, start_date, end_date):
    if df.empty or not selected_param or not start_date or not end_date:
        return {}

    mask = (df.index >= start_date) & (df.index <= end_date)
    filtered_df = df[mask]

    trace = go.Scatter(
        x=filtered_df.index,
        y=filtered_df[selected_param],
        mode="lines",
        name=selected_param,
    )

    layout = go.Layout(
        title=f"Time Series of {selected_param}",
        xaxis={"title": "Time [UTC]"},
        yaxis={"title": selected_param},
        hovermode="closest",
    )

    return {"data": [trace], "layout": layout}


if __name__ == "__main__":
    app.run_server(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
