import os
import shutil
import asyncio

class ShutilAsync:
    @staticmethod
    async def move(src:str, dst:str):
        src_directory, src_filename = os.path.split(src)
        _, src_extension = os.path.splitext(src)
        if not src_extension:
            return

        dst_directory, dst_filename = os.path.split(dst)
        _dst = dst_directory if dst_directory else dst
        _, dst_extension = os.path.splitext(dst_filename)

        if src_directory == _dst and (src_filename == dst_filename if dst_extension else False):
            return
        
        if not os.path.exists(_dst):
            await asyncio.to_thread(os.makedirs, _dst)

        path_to = os.path.join(_dst, src_filename)
        if os.path.exists(path_to):
            await asyncio.to_thread(os.remove, path_to)

        await asyncio.to_thread(shutil.move, src, dst)