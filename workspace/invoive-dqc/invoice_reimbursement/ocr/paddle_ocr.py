"""
PaddleOCR实现
"""
from paddleocr import PaddleOCR as PaddleOCR Engine
from pdf2image import convert_from_path
from pathlib import Path
from typing import List
import tempfile
import os

from .base import BaseOCR


class PaddleOCR(BaseOCR):
    """使用PaddleOCR进行文字识别"""

    def __init__(self, use_gpu: bool = False):
        """
        初始化PaddleOCR

        Args:
            use_gpu: 是否使用GPU加速
        """
        self.ocr = PaddleOCR Engine(
            use_angle_cls=True,
            lang="ch",
            use_gpu=use_gpu,
            show_log=False,
        )

    def recognize(self, file_path: str) -> str:
        """
        识别文件中的文字

        Args:
            file_path: 文件路径（支持PDF、图片）

        Returns:
            识别出的文本内容
        """
        file_path = Path(file_path)

        if file_path.suffix.lower() == ".pdf":
            return self._recognize_pdf(file_path)
        else:
            return self._recognize_image(file_path)

    def _recognize_pdf(self, pdf_path: Path) -> str:
        """识别PDF文件"""
        # 转换PDF为图片
        images = convert_from_path(pdf_path, dpi=200)

        all_text = []
        for i, image in enumerate(images):
            # 保存临时图片
            with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
                image.save(tmp.name, "JPEG")
                tmp_path = tmp.name

            # 识别
            text = self._recognize_image(Path(tmp_path))
            all_text.append(text)

            # 清理临时文件
            os.unlink(tmp_path)

        return "\n".join(all_text)

    def _recognize_image(self, image_path: Path) -> str:
        """识别图片文件"""
        result = self.ocr.ocr(str(image_path), cls=True)

        if not result or not result[0]:
            return ""

        # 提取所有文本
        lines = []
        for line in result[0]:
            if line and len(line) >= 2:
                text = line[1][0]  # (box, (text, confidence))
                lines.append(text)

        return "\n".join(lines)
