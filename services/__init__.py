from __future__ import annotations

from importlib import import_module

__all__ = [
    "SupabaseClient",
    "PdfService",
    "PushMessageService",
    "TempFileManager",
    "AssetPipeline",
    "ReportRenderer",
    "RunSelector",
    "RunSelectionError",
    "ChiShiSelection",
    "RunSelectionResult",
    "RunStatus",
    "RunReason",
    "RunBatch",
    "RunBatchReport",
    "RunReport",
    "PushRequest",
    "HistoryStore",
]


def __getattr__(name: str):
    if name == "SupabaseClient":
        return import_module(".supabase_client", __name__).SupabaseClient
    if name == "PdfService":
        return import_module(".pdf_service", __name__).PdfService
    if name == "PushMessageService":
        return import_module(".push_message_service", __name__).PushMessageService
    if name == "TempFileManager":
        return import_module(".temp_file_manager", __name__).TempFileManager
    if name == "HistoryStore":
        return import_module(".history_store", __name__).HistoryStore
    if name == "AssetPipeline":
        return import_module(".asset_pipeline", __name__).AssetPipeline
    if name == "ReportRenderer":
        return import_module(".report_renderer", __name__).ReportRenderer
    if name in {"RunSelector", "RunSelectionError", "ChiShiSelection", "RunSelectionResult"}:
        return getattr(import_module(".run_selector", __name__), name)
    if name in {"RunStatus", "RunReason", "RunBatch", "RunBatchReport", "RunReport", "PushRequest"}:
        return getattr(import_module(".models", __name__), name)
    raise AttributeError(name)
