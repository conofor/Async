import os
import asyncio
from PIL import Image
from concurrent.futures import ThreadPoolExecutor

from .LockManager import lock_manager

class DPIAsync:
    def __init__(self) -> None:
        pass
    
    @staticmethod
    async def get(image_path:str) -> tuple|None:
        async with lock_manager.get_lock_async(image_path):
            if os.path.isfile(image_path):
                try:
                    with Image.open(image_path) as img:
                        return img.info.get('dpi')
                except Exception as e:
                    print(f"Ошибка при загрузке DPI данных: {e}")

    @staticmethod
    async def set(image_path:str, dpi:tuple|int=300) -> None:
        async with lock_manager.get_lock_async(image_path):
            if os.path.isfile(image_path):
                try:
                    with Image.open(image_path) as img:
                        old_dpi = img.info.get('dpi')
                        if isinstance(dpi, int):
                            dpi = (dpi, dpi)

                        if not old_dpi == dpi:
                            loop = asyncio.get_running_loop()
                            with ThreadPoolExecutor() as pool:
                                await loop.run_in_executor(pool, lambda: img.save(image_path, dpi=dpi, quality=100))
                except Exception as e:
                    print(f"Ошибка при обновлении DPI данных: {e}")