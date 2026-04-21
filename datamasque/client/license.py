import logging

from datamasque.client.base import BaseClient, FileOrContent, UploadFile, read_file_or_content
from datamasque.client.models.license import LicenseInfo

logger = logging.getLogger(__name__)


class LicenseClient(BaseClient):
    """License management API methods. Mixed into `DataMasqueClient`."""

    def upload_license_file(self, license_file: FileOrContent) -> None:
        """
        Uploads a DataMasque license.

        Specify the path to a license (.dmlicense) filename,
        or pass a `StringIO` or `BytesIO` containing the license content.
        """

        license_file_name, content = read_file_or_content(license_file, "license.lic")
        content.seek(0)

        self.make_request(
            method="POST",
            path="/api/license-upload/",
            files=[
                UploadFile(
                    field_name="license_file",
                    filename=license_file_name,
                    content=content,
                    content_type="application/octet-stream",
                ),
            ],
        )
        logger.info("License upload successful.")

    def get_current_license_info(self) -> LicenseInfo:
        """Returns information about the license currently installed on the server."""

        response = self.make_request("GET", "/api/license/")
        return LicenseInfo.model_validate(response.json())
