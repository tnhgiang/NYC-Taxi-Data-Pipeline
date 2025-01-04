from pathlib import Path

import requests
from dagster import ConfigurableResource, InitResourceContext

from ..utils import get_current_time


class FileDownloaderReousrce(ConfigurableResource):
    file_type: str

    def _get_path(self, context: InitResourceContext) -> str:
        raise NotImplementedError

    def download(self, context: InitResourceContext, url: str) -> str:
        """Download a file from a given URL."""
        response = requests.get(url)
        if response.status_code == 200:
            file_path = self._get_path(context)
            with open(file_path, "wb") as file:
                file.write(response.content)
            context.log.debug(
                f"{self.__class__.__name__}: {self.file_type} file downloaded "
                f"successfully from {url} to {file_path}"
            )

            return file_path
        else:
            context.log.error(
                f"{self.__class__.__name__}: Failed to download {self.file_type} file "
                f"from {url}"
            )
            response.raise_for_status()


class CSVDownloaderResource(FileDownloaderReousrce):
    file_type: str = "CSV"

    def _get_path(self, context: InitResourceContext) -> str:
        """Get the temporary path for the CSV file."""
        return str(
            Path("/tmp")
            / f"csv-{get_current_time()}-{'-'.join(context.asset_key.path)}.csv"
        )


class ZipFileDownloaderResource(FileDownloaderReousrce):
    file_type: str = "ZIP"

    def _get_path(self, context: InitResourceContext) -> str:
        """Get the temporary path for the ZIP file."""
        return str(
            Path("/tmp")
            / f"shapefile-{get_current_time()}-{'-'.join(context.asset_key.path)}.zip"
        )
