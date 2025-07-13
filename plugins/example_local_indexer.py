from ifetch.plugin import BasePlugin
from pathlib import Path

class LocalIndexerPlugin(BasePlugin):
    """Example plugin that maintains a simple index of downloaded files."""

    def __init__(self):
        self.index_path = Path.home() / ".ifetch_index.txt"

    def after_download(self, remote_item, local_path, success, **kwargs):
        if success:
            with self.index_path.open("a") as f:
                f.write(f"{remote_item.name}\t{local_path}\n") 