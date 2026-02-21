"""Simulator + uploader tooling.

These modules intentionally live outside the ingestion service.
They simulate:
- a lab analyzer producing canonical CSV exports
- a middleware uploader POSTing those exports into the ingestion API
"""
