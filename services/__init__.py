from __future__ import annotations

from importlib import import_module

__all__ = ["SupabaseClient", "PdfService", "PushMessageService", "TempFileManager"]


def __getattr__(name: str):
    if name == "SupabaseClient":
        return import_module(".supabase_client", __name__).SupabaseClient
    if name == "PdfService":
        return import_module(".pdf_service", __name__).PdfService
    if name == "PushMessageService":
        return import_module(".push_message_service", __name__).PushMessageService
    if name == "TempFileManager":
        return import_module(".temp_file_manager", __name__).TempFileManager
    raise AttributeError(name)
