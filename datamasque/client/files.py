from pathlib import Path
from typing import Optional, Type, TypeVar, Union

from datamasque.client.base import BaseClient, UploadFile, read_file_or_content
from datamasque.client.models.files import DataMasqueFile
from datamasque.client.models.project import Project, ProjectId, unwrap_project_id

FileTypeT = TypeVar("FileTypeT", bound=DataMasqueFile)


class FileClient(BaseClient):
    """File-upload API methods. Mixed into `DataMasqueClient`."""

    def upload_file(
        self,
        file_type: Type[FileTypeT],
        file_name: str,
        file_path_or_content: Union[str, bytes, Path],
        project: Union[ProjectId, Project],
    ) -> FileTypeT:
        """
        Uploads a file of the given type to the DataMasque server, into the given project.

        `file_type` must be a concrete subclass of `DataMasqueFile`
        (`SeedFile`, `OracleWalletFile`, `SslZipFile`, `SnowflakeKeyFile`).
        `file_path_or_content` may be a path (as `str` or `Path`), raw `bytes`, or a file-like object.
        `project` is a `Project` or its `ProjectId`.
        """

        name, content = read_file_or_content(file_path_or_content, file_name)
        content.seek(0)

        response = self.make_request(
            "POST",
            file_type.get_url(),
            data={"name": file_name, "project": unwrap_project_id(project), **file_type.get_extra_form_data()},
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

    def list_files_of_type(
        self, file_type: Type[FileTypeT], project: Optional[Union[ProjectId, Project]] = None
    ) -> list[FileTypeT]:
        """
        Returns all files of the given type (a concrete subclass of `DataMasqueFile`).

        Only files in `project` are returned when it is given.
        `project` is a `Project` or its `ProjectId`.
        """

        params = {"project": unwrap_project_id(project)} if project is not None else None
        response = self.make_request("GET", file_type.get_url(), params=params)
        return [file_type.model_validate(file) for file in response.json()]

    def get_file_of_type_by_name(
        self, file_type: Type[FileTypeT], name: str, project: Union[ProjectId, Project]
    ) -> Optional[FileTypeT]:
        """
        Looks for a file of the given type (a concrete subclass of `DataMasqueFile`) by name in the given project.

        Returns it if found, otherwise `None`.
        """

        matching_files = [f for f in self.list_files_of_type(file_type, project) if f.name == name]
        return matching_files[0] if matching_files else None

    def upload_file_if_not_exists(
        self, file_type: Type[FileTypeT], file_path: Union[str, Path], project: Union[ProjectId, Project]
    ) -> Optional[FileTypeT]:
        """
        Upload a file only if one with the same name doesn't already exist in the project.

        Args:
            file_type: A concrete subclass of `DataMasqueFile` (e.g., SeedFile, OracleWalletFile).
            file_path: Path to the file to upload.
            project: A `Project` or its `ProjectId`.

        Returns:
            The uploaded file object if a new file was uploaded, or None if a file
            with the same name already exists in the project.
        """

        file_path = Path(file_path)
        if self.get_file_of_type_by_name(file_type, file_path.name, project) is not None:
            return None

        return self.upload_file(file_type, file_path.name, file_path, project)
