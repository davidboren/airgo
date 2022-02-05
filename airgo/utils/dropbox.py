import dropbox  # type: ignore
import os


def fetch_dropbox(file_path):
    dbx = dropbox.Dropbox(os.getenv("DROPBOX__ACCESS_TOKEN"))
    _, res = dbx.files_download(file_path)
    return res
