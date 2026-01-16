import io
from googleapiclient.http import MediaIoBaseUpload

def drive_upload_or_update_xlsx(service, folder_id: str, filename: str, xlsx_bytes: bytes):
    """
    Sube (o actualiza si ya existe) un .xlsx a la carpeta dada.
    Devuelve (file_id, action) donde action es 'created' o 'updated'.
    """
    query = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(
        q=query,
        spaces="drive",
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        pageSize=1,
    ).execute()
    files = res.get("files", [])
    file_id = files[0]["id"] if files else None

    media = MediaIoBaseUpload(
        io.BytesIO(xlsx_bytes),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        resumable=True,
    )

    if file_id:
        updated = service.files().update(
            fileId=file_id,
            media_body=media,
            supportsAllDrives=True,
        ).execute()
        return updated["id"], "updated"
    else:
        metadata = {"name": filename, "parents": [folder_id]}
        created = service.files().create(
            body=metadata,
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        ).execute()
        return created["id"], "created"
