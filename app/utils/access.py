"""
This module contains a decorator for checking access to permforming operations.
"""

from functools import wraps

import inspect
import logging
from uuid import UUID
from typing import Optional


from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from app.models.schema_models import (
    RoleTableNameModel,
    UserProjectRoleModel,
    RolePermissionModel,
    UserModel,
    DatabaseConnectionModel,
)

from app.schemas import ValidateChartAccessRequest

from app.utils.constants import Permissions as Permission

logger = logging.getLogger(__name__)


async def check_access(
    db: Session,
    token_payload: dict,
    project_id: Optional[UUID] = None,
    permission_key: str = None,
):
    """
    Check if a user has access to a project and a specific permission.
    Args:
        db (Session): The database session.
        token_payload (dict): The payload of the JWT token.
        project_id (UUID): The ID of the project.
        permission_key (str): The key of the permission.
    Returns:
        bool: True if the user has access, False otherwise.
    """
    try:
        if not token_payload:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token payload"
            )

        user_id_str = token_payload.get("sub")
        if not user_id_str:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload"
            )

        try:
            user_id = UUID(user_id_str)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid UUID format in token",
            ) from exc

        user = db.query(UserModel).filter(UserModel.id == user_id).first()
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
            )

        if user.is_super:
            return True

        # Special case: CREATE_PROJECT or DELETE_PROJECT permission
        if permission_key in [
            Permission.CREATE_PROJECT.value,
            Permission.DELETE_PROJECT.value,
            Permission.DELETE_DATASOURCE.value,
            Permission.EDIT_DATASOURCE.value,
        ]:
            roles = (
                db.query(UserProjectRoleModel)
                .filter(UserProjectRoleModel.user_id == user_id)
                .all()
            )

            if not roles:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User does not have access to create projects",
                )

            for role in roles:
                has_permission = (
                    db.query(RolePermissionModel)
                    .filter(
                        RolePermissionModel.role_id == role.role_id,
                        RolePermissionModel.permission_id
                        == Permission.CREATE_PROJECT.value,
                    )
                    .first()
                )

                if has_permission:
                    return True

            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User does not have permission to create projects",
            )

        # General permissions: require project_id and check specific permission
        role = (
            db.query(UserProjectRoleModel)
            .filter(
                UserProjectRoleModel.user_id == user_id,
                UserProjectRoleModel.project_id == project_id,
            )
            .first()
        )

        if not role:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User does not have access to this project",
            )

        has_permission = (
            db.query(RolePermissionModel)
            .filter(
                RolePermissionModel.role_id == role.role_id,
                RolePermissionModel.permission_id == Permission.ADD_DATASOURCE.value,
            )
            .first()
        )

        if not has_permission:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User does not have access to perform this operation",
            )

        return True

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.exception("Error in access control")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)
        ) from e


def ensure_datasource_access(
    db: Session, token_payload: dict, datasource_connection_id: UUID
) -> None:
    """The caller must be a super user or belong to the project that owns the datasource.

    A datasource that does not exist passes here, so the query path answers for it as it always has.
    Raises HTTPException 401 (bad token), 404 (unknown user) or 403 (not in the project).
    """
    try:
        user_id = UUID((token_payload or {}).get("sub"))
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload"
        ) from exc

    user = db.query(UserModel).filter(UserModel.id == user_id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    if user.is_super:
        return

    datasource = (
        db.query(DatabaseConnectionModel)
        .filter(DatabaseConnectionModel.id == datasource_connection_id)
        .first()
    )
    if datasource is None:
        return

    member = (
        db.query(UserProjectRoleModel)
        .filter(
            UserProjectRoleModel.user_id == user_id,
            UserProjectRoleModel.project_id == datasource.project_id,
        )
        .first()
    )
    if member is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User does not have access to this datasource",
        )


def require_permission(permission_key: str):
    """
    Decorator for checking access to a specific permission.
    Args:
        permission_key (str): The key of the permission.
    Returns:
        Callable: The decorated function.
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                sig = inspect.signature(func)
                bound_args = sig.bind_partial(*args, **kwargs)

                db = bound_args.arguments.get("db")
                token_payload = bound_args.arguments.get("token_payload")
                project_id = bound_args.arguments.get("project_id")

                if not db:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Database session not provided",
                    )

                if not token_payload:
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Authentication required",
                    )

                if permission_key == Permission.CREATE_PROJECT.value:
                    await check_access(db, token_payload, None, permission_key)
                else:
                    await check_access(db, token_payload, project_id, permission_key)

                return await func(*args, **kwargs)

            except HTTPException:
                raise
            except Exception as e:
                logger.exception("Error during permission check")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Authorization error: {str(e)}",
                ) from e

        return wrapper

    return decorator


# def validate_chart_access(data:ValidateChartAccessRequest,db:Session,user_id:dict):
#     """
#     Validate access to a chart.
#     """
#     try:
#         user_id = UUID(user_id.get("sub"))
#         if not user_id:
#             raise HTTPException(
#                 status_code=status.HTTP_401_UNAUTHORIZED,
#                 detail="Invalid token payload"
#             )
#         for table_name in data.table_names:
#             blacklist_data = db.query(BlacklistModel).filter(
#                 BlackListModel.role_id == data.role_id,
#                 BlacklistModel.table_name == table_name
#             ).first()
#             if blacklist_data:
#                 raise HTTPException(
#                     status_code=status.HTTP_403_FORBIDDEN,
#                     detail="Access denied to this table"
#                 )
#         return true
#     except Exception as e:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail=str(e)
#         )
def validate_chart_access(data: ValidateChartAccessRequest, db: Session, user_id: dict):
    """
    Validate access to a chart by checking if the user's role has permission to access the tables.
    """
    try:
        # Ensure user_id is valid and properly converted to UUID from the token
        user_id = UUID(user_id.get("sub"))

        # Loop through all table names provided in the request data
        for table_name in data.table_names:
            # Query the blacklist to check if the user has been denied access to this table
            blacklist_data = (
                db.query(RoleTableNameModel)
                .filter(
                    RoleTableNameModel.role_id == data.role_id,
                    RoleTableNameModel.table_name == table_name,
                )
                .first()
            )

            # If there is an entry in the blacklist for this table, deny access
            if blacklist_data:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Access denied to table: {table_name}",
                )

        # If no blacklist entries are found, return True indicating access is validated
        return True

    except HTTPException as http_error:
        # Catch HTTPExceptions explicitly and re-raise
        raise http_error

    except Exception as e:
        # Handle unexpected errors by raising a 400 error with the exception message
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error occurred: {str(e)}"
        ) from e
