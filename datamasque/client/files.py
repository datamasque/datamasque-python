from pathlib import Path
from typing import Optional, Type, TypeVar, Union

from datamasque.client.base import BaseClient, UploadFile, read_file_or_content
from datamasque.client.models.files import DataMasqueFile

FileTypeT = TypeVar("FileTypeT", bound=DataMasqueFile)


class FileClient(BaseClient):
    """File-upload API methods. Mixed into `DataMasqueClient`."""

    def upload_file(
        self,
        file_type: Type[FileTypeT],
        file_name: str,
        file_path_or_content: Union[str, bytes, Path],
    ) -> FileTypeT:
        """
        Uploads a file of the given type to the DataMasque server.

        `file_type` must be a concrete subclass of `DataMasqueFile`
        (`SeedFile`, `OracleWalletFile`, `SslZipFile`, `SnowflakeKeyFile`).
        `file_path_or_content` may be a path (as `str` or `Path`), raw `bytes`, or a file-like object.
        """

        name, content = read_file_or_content(file_path_or_content, file_name)
        content.seek(0)

        response = self.make_request(
            "POST",
            file_type.get_url(),
            data={"name": file_name},
            files=[
                UploadFile(
                    field_name=file_type.get_content_param_name(),
                    filename=name,
                    content=content,
                    content_type="application/octet-stream",
                ),
            ],
        )
        return file_type.model_validate(response.json())

    def delete_file_if_exists(self, file: DataMasqueFile) -> None:
        """
        Deletes a file. No-op if the file does not exist.

        `file` must be an instance of a concrete subclass of `DataMasqueFile`.
        The `file` must have its ID set.
        """

        if file.id is None:
            raise ValueError("File has not yet been created")

        # file.get_url() ends with a slash so no need to insert one before the id
        self._delete_if_exists(f"{file.get_url()}{file.id}/")

    def list_files_of_type(self, file_type: Type[FileTypeT]) -> list[FileTypeT]:
        """Returns all files of the given type (a concrete subclass of `DataMasqueFile`)."""

        response = self.make_request("GET", file_type.get_url())
        return [file_type.model_validate(file) for file in response.json()]

    def get_file_of_type_by_name(self, file_type: Type[FileTypeT], name: str) -> Optional[FileTypeT]:
        """
        Looks for a file of the given type (a concrete subclass of `DataMasqueFile`) with the given `name`.

        Returns it if found, otherwise `None`.
        """

        matching_files = [f for f in self.list_files_of_type(file_type) if f.name == name]
        return matching_files[0] if matching_files else None

    def upload_file_if_not_exists(self, file_type: Type[FileTypeT], file_path: Union[str, Path]) -> Optional[FileTypeT]:
        """
        Upload a file only if one with the same name doesn't already exist.

        Args:
            file_type: A concrete subclass of `DataMasqueFile` (e.g., SeedFile, OracleWalletFile).
            file_path: Path to the file to upload.

        Returns:
            The uploaded file object if a new file was uploaded, or None if a file
            with the same name already exists.
        """

        file_path = Path(file_path)
        if self.get_file_of_type_by_name(file_type, file_path.name) is not None:
            return None

        return self.upload_file(file_type, file_path.name, file_path)
