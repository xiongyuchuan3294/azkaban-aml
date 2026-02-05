"""
OCR模块
"""
from .base import BaseOCR
from .paddle_ocr import PaddleOCR

__all__ = ["BaseOCR", "PaddleOCR"]
