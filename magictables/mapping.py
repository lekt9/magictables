# mapping.py

from .database import magic_db


def store_mapping(mapping_name: str, mapping: dict):
    magic_db.store_mapping(mapping_name, mapping)


def get_stored_mapping(mapping_name: str) -> dict:
    return magic_db.get_mapping(mapping_name)
