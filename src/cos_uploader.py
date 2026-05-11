"""
腾讯云 COS 图片上传模块

将生成的图片上传到 COS 对象存储，返回可直接访问的 URL，
用于替代 base64 内嵌传输，大幅减少跨境传输体积。

环境变量配置:
    COS_SECRET_ID:   腾讯云 API SecretId
    COS_SECRET_KEY:  腾讯云 API SecretKey
    COS_REGION:      存储桶地域，如 ap-guangzhou
    COS_BUCKET:      存储桶名称（含 APPID），如 my-bucket-1250000000
    COS_URL_PREFIX:  (可选) 自定义访问域名，如 https://img.example.com
                     默认使用 COS 官方域名
    COS_PATH_PREFIX: (可选) 对象键前缀，默认 "generated/"

如果必要变量未配置，模块将静默跳过，返回 None。
"""

import base64
import os
import uuid
from datetime import datetime
from typing import Optional

from log import log


# 延迟初始化的 COS 客户端
_cos_client = None
_cos_bucket: Optional[str] = None
_cos_url_prefix: Optional[str] = None
_cos_path_prefix: str = "generated/"
_initialized = False


def _ensure_initialized():
    """延迟初始化 COS 客户端（仅在首次调用时执行）"""
    global _cos_client, _cos_bucket, _cos_url_prefix, _cos_path_prefix, _initialized

    if _initialized:
        return
    _initialized = True

    secret_id = os.getenv("COS_SECRET_ID", "").strip()
    secret_key = os.getenv("COS_SECRET_KEY", "").strip()
    region = os.getenv("COS_REGION", "").strip()
    bucket = os.getenv("COS_BUCKET", "").strip()

    if not all([secret_id, secret_key, region, bucket]):
        log.info("[COS] 未配置 COS 环境变量，图片将使用 base64 内嵌传输")
        return

    try:
        from qcloud_cos import CosConfig, CosS3Client

        config = CosConfig(
            Region=region,
            SecretId=secret_id,
            SecretKey=secret_key,
            Timeout=30,
        )
        _cos_client = CosS3Client(config)
        _cos_bucket = bucket
        _cos_url_prefix = os.getenv("COS_URL_PREFIX", "").strip().rstrip("/")
        _cos_path_prefix = os.getenv("COS_PATH_PREFIX", "generated/").strip()

        if not _cos_url_prefix:
            _cos_url_prefix = f"https://{bucket}.cos.{region}.myqcloud.com"

        log.info(f"[COS] 初始化成功: bucket={bucket}, region={region}, url_prefix={_cos_url_prefix}")
    except ImportError:
        log.warning("[COS] cos-python-sdk-v5 未安装，跳过 COS 初始化")
    except Exception as e:
        log.error(f"[COS] 初始化失败: {e}")


def upload_base64_image(base64_data: str, mime_type: str = "image/png") -> Optional[str]:
    """
    将 base64 编码的图片上传到 COS

    Args:
        base64_data: base64 编码的图片数据
        mime_type: MIME 类型，如 "image/png"

    Returns:
        图片 URL，如果上传失败或未配置则返回 None
    """
    _ensure_initialized()

    if _cos_client is None:
        return None

    # 生成唯一文件名: generated/2024-01-15/abc123def4.png
    ext = mime_type.split("/")[-1] if "/" in mime_type else "png"
    date_prefix = datetime.now().strftime("%Y-%m-%d")
    filename = f"{uuid.uuid4().hex[:10]}.{ext}"
    key = f"{_cos_path_prefix}{date_prefix}/{filename}"

    try:
        image_bytes = base64.b64decode(base64_data)

        _cos_client.put_object(
            Bucket=_cos_bucket,
            Body=image_bytes,
            Key=key,
            ContentType=mime_type,
        )

        url = f"{_cos_url_prefix}/{key}"
        size_kb = len(image_bytes) // 1024
        log.info(f"[COS] 上传成功: {key} ({size_kb}KB) → {url}")
        return url

    except Exception as e:
        log.warning(f"[COS] 上传失败，回退到 base64: {e}")
        return None


def is_enabled() -> bool:
    """检查 COS 是否已配置并可用"""
    _ensure_initialized()
    return _cos_client is not None
